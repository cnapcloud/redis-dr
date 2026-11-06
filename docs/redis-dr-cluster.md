최종 목표
Redis Cluster A(active)와 B(standby)를 독립된 클러스터 2개로 두고, A의 증분 데이터를 shard별로 B에 실시간 복제하는 DR을 구성합니다. Sentinel 엔진(redis-dr-sentinel.md)을 shard 단위로 확장했습니다.

범위
- 구현: `redis-dr` Cluster 모드(`REDIS_DR_MODE=cluster`)와 테스트 도구
- 환경: k3s(node 3개), Redis 7.0.15, redis-operator 0.27.0
- 시험 범위: 4장 표 기준, 미검증 항목은 별도 표시

검토한 다른 안
- A 클러스터에 B 노드를 replica로 연결: 장애 영역 공유, DC 간 지연 유입으로 제외
- RedisShake cluster 모드: 체크포인트 이어받기, topology 변경 대응 없음으로 제외

## 1. 테스트 클러스터 구성

```
Cluster A (active, namespace redis-dr-a)

 Shard 0  slots 0-5460        Shard 1  slots 5461-10922     Shard 2  slots 10923-16383
 +--------------------+       +--------------------+        +--------------------+
 | redis-a-leader-0   |       | redis-a-leader-1   |        | redis-a-leader-2   |
 |   MASTER           |       |   MASTER           |        |   MASTER           |
 |      | replication |       |      | replication |        |      | replication |
 |      v             |       |      v             |        |      v             |
 | redis-a-follower-0 |       | redis-a-follower-1 |        | redis-a-follower-2 |
 |   REPLICA          |       |   REPLICA          |        |   REPLICA          |
 +--------|-----------+       +--------|-----------+        +--------|-----------+
          | PSYNC (replid+offset)       | PSYNC                       | PSYNC
          v                             v                             v
 +-------------------------------------------------------------------------------+
 | redis-dr (namespace redis-dr-engine)              DR engine 1 pod             |
 |   Worker 0                   Worker 1                   Worker 2              |
 +--------|-----------------------------|-----------------------------|----------+
          | write + checkpoint          |                             |
          v                             v                             v
 +--------------------+       +--------------------+        +--------------------+
 | redis-b-leader-0   |       | redis-b-leader-1   |        | redis-b-leader-2   |
 |   MASTER           |       |   MASTER           |        |   MASTER           |
 |      v             |       |      v             |        |      v             |
 | redis-b-follower-0 |       | redis-b-follower-1 |        | redis-b-follower-2 |
 |   REPLICA          |       |   REPLICA          |        |   REPLICA          |
 +--------------------+       +--------------------+        +--------------------+
Cluster B (standby, namespace redis-dr-b)
```

| 항목 | 값 |
|---|---|
| 클러스터 | A, B 각 3 shard, shard당 master 1 + replica 1 (pod 6개) |
| node | k3s 3개 (control-plane 1, worker 2) |
| 영속성 | PVC (data, nodes.conf), AOF + RDB (operator 기본값) |
| 엔진 | A shard 수만큼 worker, 기본 구성은 pod 1개 |

## 2. engine 구성

### 2.1 구조

- worker 1개 = A shard 1개 = B shard 1개 (slot 범위로 매칭, pod 이름 아님)
- 소스는 A shard의 replica, master 미사용
- 토폴로지는 `CLUSTER SHARDS`로 조회
- B 쓰기는 Lua 1회로 데이터와 체크포인트를 함께 갱신
- 스크립트 첫 줄 `#!lua flags=allow-cross-slot-keys`
- 체크포인트 key는 B shard의 slot에 해시되는 태그 사용 (`{태그}:dr:checkpoint:shard-i`)

| 용어 | 의미 |
|---|---|
| 체크포인트 | B에 저장한 replid + offset, "여기까지 적용" |
| 백로그 | 소스 노드가 보관하는 최근 복제 스트림 (기본 1MB), 이어받기에 필요 |
| full sync | B shard를 비우고 소스의 스냅샷을 적재한 뒤 증분 시작 |

### 2.2 동작

| 상황 | 동작 |
|---|---|
| 첫 시작, 체크포인트 없음 | 해당 shard full sync |
| 엔진 재시작 | 체크포인트에서 이어받기 |
| full sync 도중 종료 | `SYNCING` 상태 확인 후 처음부터 재수행 |
| 소스 replica 장애 | 같은 shard의 다른 replica로 전환 |
| 소스 replica 승격 | `replid2`로 체크포인트 재정렬 후 이어받기 |
| replica가 있지만 모두 체크포인트 서빙 불가 | 20초 후 체크포인트 폐기, 해당 shard full sync |
| A 이력과 체크포인트 불일치 (DIVERGED) | 해당 shard full sync |
| A·B shard 수, slot 범위 불일치 | 로그 후 종료 (5초 주기 검사) |

### 2.3 설정 (환경변수)

Sentinel과 Cluster를 한 프로그램이 처리하고 `REDIS_DR_MODE=cluster`로 고릅니다. 모든 설정과 기본값은 `application.yaml`에 있고, 환경변수 `REDIS_DR_` + 프로퍼티 경로(대문자, 점과 하이픈은 `_`)로 덮어씁니다.

| 이름 | 기본값 |
|---|---|
| `REDIS_DR_MODE` | `sentinel` (여기서는 `cluster`로 지정) |
| `REDIS_DR_CLUSTER_SOURCE_NODES` | `redis-a-leader.redis-dr-a:6379,redis-a-follower.redis-dr-a:6379` |
| `REDIS_DR_CLUSTER_TARGET_NODES` | `redis-b-leader.redis-dr-b:6379,redis-b-follower.redis-dr-b:6379` |
| `REDIS_DR_SCHEDULER_LAYOUT_CHECK_MS` | 5000 |
| `REDIS_DR_CLUSTER_FULLSYNC_FALLBACK_SECONDS` | 20 |

### 2.4 코드 구조

- 복제 로직은 `DrEngine` 하나이고, 모드별 차이는 Site A 조회(`SourceTopology`)와 Site B 쓰기(`TargetWriter`) 구현에만 있습니다.
- `redis.dr.service.impl.cluster`: `ClusterSourceTopologyImpl`, `ClusterTargetWriterImpl`, `LayoutValidatorImpl`
- `redis.dr.service.impl.sentinel`: Sentinel 모드 구현
- 운영자 API: `redis.dr.controller` (진행 현황, 중지/재개, 풀 싱크)
- 테스트 도구: `redis.dr.runner.ToolRunner` (`load`, `write`, `verify`), `--spring.profiles.active=tool`로 실행

## 3. 설치

### 3.1 cluster

```bash
helm repo add ot-helm https://ot-container-kit.github.io/helm-charts/
helm upgrade --install redis-operator ot-helm/redis-operator \
  -n redis-operator --create-namespace --wait
helm upgrade --install redis-a ot-helm/redis-cluster \
  -n redis-dr-a --create-namespace -f values-a.yaml
helm upgrade --install redis-b ot-helm/redis-cluster \
  -n redis-dr-b --create-namespace -f values-b.yaml
```

`values-a.yaml` (B는 `redis-a`를 `redis-b`로 치환)

```yaml
redisCluster:
  name: redis-a
  clusterSize: 3
  clusterVersion: v7
  image: quay.io/opstree/redis
  tag: v7.0.15
  persistenceEnabled: true
  leader:
    replicas: 3
    topologySpreadConstraints:
      - maxSkew: 1
        topologyKey: kubernetes.io/hostname
        whenUnsatisfiable: ScheduleAnyway
        labelSelector:
          matchLabels: {cluster: redis-a, role: leader}
  follower:
    replicas: 3
    topologySpreadConstraints:
      - maxSkew: 1
        topologyKey: kubernetes.io/hostname
        whenUnsatisfiable: ScheduleAnyway
        labelSelector:
          matchLabels: {cluster: redis-a, role: follower}

externalConfig:
  enabled: true
  data: |
    cluster-node-timeout 5000

storageSpec:
  volumeClaimTemplate:
    spec:
      accessModes: ["ReadWriteOnce"]
      resources: {requests: {storage: 1Gi}}
  nodeConfVolume: true
  nodeConfVolumeClaimTemplate:
    spec:
      accessModes: ["ReadWriteOnce"]
      resources: {requests: {storage: 1Gi}}
```

확인 (`cluster_state:ok`, `cluster_known_nodes:6`)

```bash
kubectl exec -n redis-dr-a redis-a-leader-0 -c redis-a-leader -- \
  redis-cli cluster info
```

### 3.2 engine

엔진은 pod IP에 접근해야 해서 클러스터 안에서 Deployment로 실행합니다. 한 A→B 쌍에는 하나만 실행하므로 `replicas: 1`, `Recreate` 전략을 씁니다. 레지스트리가 없는 시험 환경이라 이미지를 빌드해 각 노드의 containerd에 직접 넣었습니다.

```bash
docker build -t redis-dr:e2e .
docker save redis-dr:e2e | limactl shell <vm> sudo k3s ctr images import -   # 노드마다 반복
kubectl create namespace redis-dr-engine
kubectl apply -f redis-dr.yaml
```

`redis-dr.yaml`

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: redis-dr
  namespace: redis-dr-engine
spec:
  replicas: 1
  strategy:
    type: Recreate
  selector:
    matchLabels: { app: redis-dr }
  template:
    metadata:
      labels: { app: redis-dr }
    spec:
      containers:
        - name: redis-dr
          image: redis-dr:e2e
          imagePullPolicy: Never
          env:
            - name: REDIS_DR_MODE
              value: cluster
```

`SOURCE_NODES`/`TARGET_NODES`는 기본값이 이 시험 환경(`redis-a-*.redis-dr-a`, `redis-b-*.redis-dr-b`)과 같아 지정하지 않았습니다.

운영자 API는 pod 안의 loopback에서만 듣습니다. 현황 확인과 중지/재개, 풀 싱크는 아래처럼 접근합니다.

```bash
kubectl exec -n redis-dr-engine deploy/redis-dr -- curl -s localhost:8081/api/v1/status
```

테스트 도구는 같은 이미지로 일회성 pod를 띄워 실행합니다.

```bash
kubectl run t-verify -n redis-dr-engine --rm -i --restart=Never \
  --image=redis-dr:e2e --image-pull-policy=Never --env=REDIS_DR_MODE=cluster -- \
  --spring.profiles.active=tool verify
```

## 4. 테스트 시나리오와 결과

| # | 시나리오 | 방법 | 결과 |
|---|---|---|---|
| 1 | `allow-cross-slot-keys` | B master에서 KEYS에 선언하지 않은 다른 slot key에 접근하는 EVAL | 통과, 플래그 없으면 에러, 있으면 허용 |
| 2 | 첫 full sync | string(TTL), hash, set, zset, list 적재 후 엔진 시작 | 통과, A=B (5만 개 재시험) |
| 3 | 소스 replica 삭제 | 쓰기 중 pod 삭제 | 통과, 5장 참고 |
| 4 | master 삭제 (자동 failover) | 쓰기 중 pod 삭제 | 통과, 체크포인트 재정렬, 쓰기 에러 0, A=B |
| 5 | 수동 `CLUSTER FAILOVER` | 쓰기 중 실행 | 통과, 풀 싱크 없이 이어받기, 쓰기 에러 0, A=B |
| 6 | A resharding | slot 100개 이동 (쓰기 없음) | 엔진 종료 코드 2, B도 같은 배치로 맞추면 기동, 영향 shard 자동 full sync, A=B |
| 7 | 엔진 재시작, full sync 도중 종료 | pod 강제 삭제 | 통과, `SYNCING` 감지 후 재수행, A=B |
| 8 | A·B 비교 | `Tool verify` | 사용 (RedisFullCheck 미사용) |
| 9 | replica 2개 전환 | 쓰기 중 소스 replica 삭제 | 통과, 3초 만에 다른 replica로 증분 이어받기, 쓰기 에러 0, A=B |
| 10 | master 비정상 종료 | 쓰기 중 `kill -9` 후 빠른 재시작 | 통과, 해당 shard만 full sync, 확인된 쓰기 유실 없음, A=B |

- 4번: 체크포인트 재정렬 후 옛 master가 replica로 재합류하면 5장 A 경우와 같이 20초 후 full sync
- 4번: 이전 결과(체크포인트 폐기 기능 추가 전)는 해당 shard가 멈췄음
- 6번: 엔진은 `slot layout differs ... -> not starting`을 남기고 종료하고, 재시작해도 배치가 같아지기 전에는 반복 종료 (자동 복구 없음)
- 6번: B를 같은 배치로 reshard한 뒤 slot 범위가 바뀐 shard 2개는 체크포인트가 없어 full sync, 나머지 shard는 이어받기. 이후 partition 번호는 slot 범위 순서
- 6번: 이전에 겪은 MIGRATE 타임아웃, 반복 failover는 이번에 재현되지 않음. 쓰기 없이 시험했으므로 쓰기 중 resharding은 미검증
- 7번: 강제 삭제 직후 B 체크포인트가 `SYNCING`, 재시작 로그 `checkpoint in B is not usable, full sync: state=SYNCING`
- 9번: shard당 replica 2개로 만들려면 follower를 6개로 늘린 뒤 `CLUSTER REPLICATE`로 분포를 맞춰야 함 (6장)
- 10번: master가 AOF로 failover 없이 재기동하면 새 replid가 되어 replica가 full sync를 하고, 엔진은 `source history diverged`로 판단 (WARN)

운영자 API도 함께 시험
- `status`, partition 조회, 404, 입력 검증 400, `pause`/`resume`/`resume-check`, `full-sync`
- 풀 싱크 중 `pause`는 409(`force=true`로 중지), `SYNCING` 상태의 `resume`은 409
- 중지한 채 엔진을 재시작하면 중지 상태로 시작 (표시는 B에 저장)
- B master에 OOM을 걸면 `WAITING_TARGET`, 해제하면 저장된 체크포인트부터 이어받기. 이 동안 `pause`는 503

`Tool verify` 비교 방식
- A의 모든 key를 B와 type, 값, TTL로 비교
- B에만 있는 key 검사 (체크포인트 key 제외)
- 남은 TTL 120초 미만 key 제외 (A·B 만료 시각 차이)
- key 개수는 비교하지 않음 (스캔 중 TTL key 만료)
- 쓰기 종료 후 실행, DR 지연 대기는 없음

## 5. replica 장애 시 DR

| 경우 | 동작 | DR 끊김 | 비고 |
|---|---|---|---|
| A. replica 1개, AOF (기본) | replica가 master full sync, 엔진이 20초 후 B shard full sync | 약 40초 | 검증 |
| B. replica 1개, RDB | replica 부분 재동기화, 엔진 증분 이어받기 | 없음 | 정상 종료만 검증 |
| C. replica 2개, AOF | 다른 replica로 전환, 증분 이어받기 | 없음 | 검증, pod 9개 |
| D. master failover | `replid2`로 체크포인트 재정렬 | 없음 | 자동·수동 failover 검증 (이후 replica 재합류는 A 경우와 동일) |

AOF에서 full sync가 필요한 이유
- AOF에는 replid/offset 없음, 재시작 replica는 `replid:1`로 요청
- master가 거절하고 full sync, 백로그가 스냅샷 시점부터 시작
- 체크포인트 이후 구간이 없어 엔진이 해당 replica 거부

RDB를 기본으로 하지 않는 이유
- master 비정상 종료 후 빠른 재시작 시 마지막 저장 이후 유실 (최대 수 분)
- 옛 데이터로 뜬 master에 맞춰 DR도 B를 덮어씀
- 차트가 leader/follower 설정을 공유해 replica만 RDB로 분리 불가

기본 구성안
- AOF + shard당 replica 1개
- replica 재시작 시 해당 shard만 B full sync (약 40초)
- 끊김 없는 DR이 필요하면 replica 2개 (C, 시험 완료)

## 6. 주의점

- pod 이름은 `redis-a-leader-N` / `follower-N`, shard i = `leader-i` + `follower-i`
- 같은 shard의 master·replica node 분리를 values로 보장 불가
- 분리 필요 시 `CLUSTER REPLICATE`로 수동 조정, pod 재생성 후 재점검
- follower 수를 늘리면(helm upgrade) 새 replica가 shard에 고르게 붙지 않음 (시험에서 shard별 1/2/3개), `CLUSTER REPLICATE`로 shard당 같은 수로 조정
- A·B의 shard 수와 slot 배치 동일 유지
- resharding 미지원 (엔진 종료 코드 2), B 배치를 맞추면 slot 범위가 바뀐 shard는 자동으로 full sync (B가 비워짐)
- 엔진에 데이터 정합성 검사 없음 (`Tool verify`는 외부 도구)
- 비동기 복제로 A의 마지막 쓰기 일부는 B에 없을 수 있음
- Redis 7 이상 필수 (`COMMAND LIST`, `allow-cross-slot-keys`)
- 엔진 중복 실행 방지 lock 없음
- 소스 연결이 약 1분 주기로 EOF 후 재연결 (원인 미확인, 유실 없음)
- 인증 미설정, 운영에서는 `redisSecret` 또는 ACL 필요
