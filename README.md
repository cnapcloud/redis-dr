# redis-dr

## 개요

운영 중인 Redis(Site A)의 데이터를 재해복구(DR)용 Redis(Site B)로 실시간 복제하는 프로그램입니다. 이 문서에서 `redis-dr`은 이 프로그램, 또는 실행 중인 프로세스를 가리킵니다. Redis Sentinel 구성과 Redis Cluster 구성을 모두 지원하며, 같은 프로그램에서 `redis-dr.mode` 설정만 바꿔 선택합니다.

- Site A의 **replica 한 곳**에서 복제 스트림(PSYNC)을 읽어 Site B master에 일반 write 클라이언트로 씁니다. Site A master에는 붙지 않으므로 Sentinel/Cluster가 redis-dr을 승격 후보로 보지 않습니다.
- 복제 위치(체크포인트)는 **Site B 안에** 데이터와 같은 Lua 스크립트로 함께 저장됩니다. redis-dr을 재시작하거나 소스 replica가 바뀌어도 풀 싱크 없이 이어받습니다.
- Site B의 master에는 복제 설정(`REPLICAOF`)을 건드리지 않고 일반 클라이언트처럼 쓰기만 합니다. Site B에서 failover로 master가 바뀌면 Site B의 Sentinel(또는 클러스터)에서 새 주소를 받아 따라갑니다.
- Site B에 쓰지 못하거나 읽을 Site A replica가 없어도 **프로세스는 종료하지 않고** 원인을 로그와 상태 API에 남긴 채 기다리다가, 문제가 풀리면 저장된 체크포인트부터 이어갑니다.
- 진행 현황 조회, 복제 중지/재개, 풀 싱크 강제는 [운영자 API](#운영자-api)(REST, Swagger UI)로 합니다.

```
 Site A (active)                                        Site B (DR standby)
┌───────────────────────────┐   ┌──────────────────┐   ┌───────────────────────────┐
│ master                    │   │                  │   │ replica                   │
│ replica 1 (source)        │──►│     redis-dr     │──►│ master  <-- writes        │
│ replica 2                 │   │                  │   │ + checkpoint kept in B    │
└───────────────────────────┘   └──────────────────┘   └───────────────────────────┘
 Sentinel A / CLUSTER SHARDS                            Sentinel B / CLUSTER SHARDS
  picks the source replica                               tracks the B master
```

| | Sentinel 모드 | Cluster 모드 |
|---|---|---|
| 복제 단위 | Site A 전체 1개 | Site A의 shard마다 1개, Site A shard *i* → Site B shard *i* |
| Site A 토폴로지 조회 | Site A의 Sentinel | `CLUSTER SHARDS` |
| Site B master 추적 | Site B의 Sentinel | `CLUSTER SHARDS` |
| 지원 db | 모든 db(`SELECT` 추적) | db 0만 |
| shard 식별 | - | slot 범위(Pod 이름 아님) |

## 요구사항

| 항목 | 내용 |
|---|---|
| Redis | Site A와 Site B 모두 **7.0 이상** (소스에서 `COMMAND LIST`를 사용하고, Cluster 모드는 `allow-cross-slot-keys` Lua 플래그를 사용합니다) |
| Site A | master 1대와 **replica 1대 이상**. redis-dr은 replica에서만 읽으므로 replica가 없으면 복제가 시작되지 않고 `waiting for source`로 대기합니다. replica가 생기면 시작합니다. |
| Site B | Sentinel 모드: master와 Site B의 Sentinel. Cluster 모드: Site A와 **같은 shard 수와 slot 범위**의 클러스터 |
| 네트워크 | redis-dr이 실행되는 곳에서 Site A의 replica와 Site B의 master에 직접 접속할 수 있어야 합니다. Sentinel/클러스터가 알려주는 **IP**로 접속합니다. |
| 빌드 | JDK 17 (Gradle wrapper 포함) |
| 실행 | JRE 17 또는 Docker |

## 설치

### 소스에서 빌드

```bash
./gradlew build installDist        # 테스트 포함 빌드 + 실행 파일 생성
build/install/redis-dr/bin/redis-dr
```

`installDist` 결과(`build/install/redis-dr/`)를 서버에 복사해서 실행해도 됩니다. 설정은 아래 [설정](#설정)을 따릅니다.

### Docker

```bash
docker build -t redis-dr:0.1.0 .

docker run -d --name redis-dr \
  -e REDIS_DR_SENTINEL_SOURCE_NODES=sentinel-a.site-a.example:26379 \
  -e REDIS_DR_SENTINEL_TARGET_NODES=sentinel-b.site-b.example:26379 \
  redis-dr:0.1.0
```

위 두 값은 `application.yaml`의 기본값(`sentinel-a:26379`, `sentinel-b:26379`)을 환경변수로 덮어쓰는 예시입니다. 기본값과 같다면 환경변수를 줄 필요가 없습니다.

이미지는 JDK로 빌드하고 JRE로 실행하며, 비root 사용자(uid 10001)로 동작합니다.

[운영자 API](#운영자-api)는 기본적으로 컨테이너 안의 `127.0.0.1`에서만 듣기 때문에 `-p 8081:8081`만으로는 호스트에서 접근할 수 없습니다. 호스트에서 쓰려면 컨테이너 안의 주소를 열고, 호스트 쪽은 loopback에만 게시하세요. 인증이 없으므로 `-p 8081:8081`처럼 모든 인터페이스에 게시하지 마세요.

```bash
docker run -d --name redis-dr -e SERVER_ADDRESS=0.0.0.0 -p 127.0.0.1:8081:8081 … redis-dr:0.1.0
```

### Kubernetes 예시

아래는 구성 예시입니다. 이 매니페스트 그대로(Sentinel 모드)를 실행해 본 것은 아니고, 같은 형태의 Deployment를 `REDIS_DR_MODE=cluster`와 기본 주소로 k3s 클러스터(Site A/B 각 3 shard)에서 실행해 복제와 [운영자 API](#운영자-api)를 시험했습니다. **한 Site A → Site B 쌍에는 redis-dr을 하나만 실행하세요.** 같은 체크포인트를 두 인스턴스가 동시에 쓰는 구성은 지원하지 않으므로, `replicas: 1`과 `Recreate` 전략으로 롤아웃 중에도 겹치지 않게 합니다. 예전 방식의 다른 복제 프로세스가 같은 Site B에 쓰고 있지 않은지도 확인하세요.

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: redis-dr
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
          image: your-registry/redis-dr:0.1.0
          env:
            - name: REDIS_DR_MODE
              value: sentinel
            - name: REDIS_DR_SENTINEL_SOURCE_NODES
              value: sentinel-a.redis-dr-a:26379
            - name: REDIS_DR_SENTINEL_TARGET_NODES
              value: sentinel-b.redis-dr-b:26379
```

운영자 API는 기본적으로 Pod 안의 loopback에서만 듣습니다. `kubectl -n <ns> port-forward deploy/redis-dr 8081:8081` 후 `http://localhost:8081/swagger-ui.html`로 접근하세요(Cluster 모드로 k3s 클러스터에서 시험한 방법입니다).

## 설정

모든 설정과 기본값은 [application.yaml](src/main/resources/application.yaml)에 있고, 아래 세 가지 방법으로 덮어씁니다. 나중 것이 앞의 것을 덮어씁니다.

1. **기본값**: jar에 들어 있는 `application.yaml`
2. **외부 설정 파일**: 바꿀 항목만 담은 `application.yaml`을 두고 `SPRING_CONFIG_ADDITIONAL_LOCATION=file:/config/`로 지정합니다. ConfigMap을 마운트할 때 편합니다.
3. **환경변수**: `REDIS_DR_` + 프로퍼티 경로를 대문자로 쓰고 점과 하이픈은 `_`로 바꿉니다. 목록은 쉼표로 구분합니다.

```
redis-dr.mode                               ->  REDIS_DR_MODE
redis-dr.sentinel.source.nodes              ->  REDIS_DR_SENTINEL_SOURCE_NODES
redis-dr.sentinel.fullsync-fallback-seconds ->  REDIS_DR_SENTINEL_FULLSYNC_FALLBACK_SECONDS
```

### 설정 항목

`source`는 Site A(운영), `target`은 Site B(DR)입니다. 주소는 모두 `host:port` 형식이며, 형식이 틀리거나 비어 있으면 시작할 때 오류가 납니다.

| 프로퍼티 | 기본값 | 설명 |
|---|---|---|
| `redis-dr.mode` | `sentinel` | `sentinel` 또는 `cluster` |
| `redis-dr.sentinel.source.nodes` | `sentinel-a:26379` | Site A의 Sentinel 노드들. 앞에서부터 조회해 처음 응답하는 노드를 씁니다. |
| `redis-dr.sentinel.source.master` | `mymaster` | Site A의 Sentinel이 감시하는 master 이름 |
| `redis-dr.sentinel.source.domain-suffix` | `""` | Sentinel이 점이 하나인 짧은 호스트명(`redis-a-0.redis-a`)으로 알려줄 때 뒤에 붙일 도메인 |
| `redis-dr.sentinel.target.nodes` / `.master` / `.domain-suffix` | `sentinel-b:26379` / `mymaster` / `""` | Site B의 Sentinel. 의미는 `source`와 같고, Site B master 주소를 추적합니다. |
| `redis-dr.sentinel.fullsync-fallback-seconds` | `20` | 아래 [풀 싱크 폴백](#풀-싱크-폴백) 참고. `0`이면 사용 안 함 |
| `redis-dr.cluster.source.nodes` | `redis-a-leader.redis-dr-a:6379,redis-a-follower.redis-dr-a:6379` | Site A 클러스터의 아무 노드들. `CLUSTER SHARDS`를 처음 응답하는 노드에 묻습니다. |
| `redis-dr.cluster.target.nodes` | `redis-b-leader.redis-dr-b:6379,redis-b-follower.redis-dr-b:6379` | Site B 클러스터의 아무 노드들 |
| `redis-dr.cluster.fullsync-fallback-seconds` | `20` | Cluster 모드의 폴백 시간. `0`이면 사용 안 함 |
| `server.port` | `8081` | [운영자 API](#운영자-api)의 포트 |
| `server.address` | `127.0.0.1` | 운영자 API가 듣는 주소. **인증이 없으므로** 기본은 loopback만 엽니다. 바꾸려면 아래 [운영자 API](#운영자-api)의 주의를 읽으세요. |
| `redis-dr.replication.source-ping-period-seconds` | `10` | **Site A** Redis의 `repl-ping-replica-period` 값. redis-dr이 설정하는 값이 아니라 A가 쓰는 값을 **선언**하는 것이므로 A와 같게 두세요. 쓰기가 없을 때 복제 소켓에 오는 데이터는 이 PING뿐입니다. Site B에는 해당하지 않습니다. |
| `redis-dr.replication.read-timeout-seconds` | `30` | 소스 replica와의 복제 소켓이 이 시간 동안 아무것도 받지 못하면 세션을 닫고 소스를 다시 고릅니다. 죽은 연결을 알아채는 시간이기도 합니다. **ping 주기의 2배보다 커야** 합니다. 아니면 경고를 남기고 `max(30, 3 × ping 주기)`를 씁니다. `0`(타임아웃 없음)은 받지 않습니다. |
| `redis-dr.scheduler.worker-retry-ms` | `200` | worker 단계 사이 간격(세션과 세션 사이의 휴지 시간 포함) |
| `redis-dr.scheduler.source-watch-ms` | `1000` | 소스 replica가 master로 승격됐는지 확인하는 주기 |
| `redis-dr.scheduler.wait-log-ms` | `20000` | Site B에 쓰지 못하거나 읽을 Site A replica가 없는 동안, 같은 대기 사유를 로그에 다시 남기는 주기. `0`이면 반복하지 않습니다. |
| `redis-dr.scheduler.stat-report-ms` | `5000` | `stat:` 로그 주기. 운영자 API 상태(B의 체크포인트, 지연, 처리량)를 갱신하는 주기이기도 합니다. |
| `redis-dr.scheduler.layout-check-ms` | `5000` | Cluster 모드에서 Site A/B shard 수와 slot 범위를 비교하는 주기 |

로그 형식은 `logging.pattern.console`로 바꿀 수 있고, 레벨은 Spring Boot 표준 방식으로 조정합니다. 예를 들어 `LOGGING_LEVEL_REDIS_DR=WARN`이면 이 앱의 INFO 로그를 숨깁니다.

### 설정 예시

**Sentinel 모드** (기본). Sentinel이 짧은 호스트명을 알려주는 환경이면 `domain-suffix`를 함께 지정합니다.

```bash
REDIS_DR_SENTINEL_SOURCE_NODES=sentinel-a-0:26379,sentinel-a-1:26379,sentinel-a-2:26379
REDIS_DR_SENTINEL_SOURCE_DOMAIN_SUFFIX=.redis-dr-a.svc.cluster.local
REDIS_DR_SENTINEL_TARGET_NODES=sentinel-b:26379
```

**Cluster 모드**

```bash
REDIS_DR_MODE=cluster
REDIS_DR_CLUSTER_SOURCE_NODES=redis-a-0.redis-a:6379,redis-a-1.redis-a:6379
REDIS_DR_CLUSTER_TARGET_NODES=redis-b-0.redis-b:6379,redis-b-1.redis-b:6379
```

## 실행과 확인

### 정상 기동

시작하면 Site B의 체크포인트를 읽고 Site A에서 소스 replica를 고른 뒤 복제를 시작합니다. Cluster 모드는 먼저 Site A/B 레이아웃을 확인하고 shard마다 `[shard N]` 접두어가 붙습니다.

처음 실행(체크포인트 없음)이면 풀 싱크를 하고, 이후에는 증분 복제로 이어집니다.

```
INFO  no checkpoint in B, full sync
INFO  source selected: host=10.0.0.5 mode=full_sync elapsed_ms=6
INFO  full sync started: B flushed, DR unavailable until the snapshot is loaded
INFO  full sync done: keys=… elapsed_ms=… replid=… offset=…
INFO  stat: src=10.0.0.5 replid=… offset=… cp=OK@… applied=… restored=…
```

`stat:` 줄은 5초마다 나옵니다. `cp=OK@…`가 Site B에 저장된 체크포인트이고 `offset`을 따라가며 증가하면 정상입니다. `applied`는 Site B에 반영한 명령 수, `restored`는 풀 싱크로 복원한 키 수입니다.

> **첫 실행의 풀 싱크는 Site B를 비웁니다(`FLUSHALL`).** 데이터가 들어 있는 Site B에 처음 붙이기 전에 확인하세요. Cluster 모드는 해당 shard의 Site B master만 비웁니다.

### 로그 읽는 법

| 레벨 | 메시지 | 의미 |
|---|---|---|
| INFO | `resume from persisted checkpoint: replid=… offset=…` | Site B에 저장된 체크포인트에서 이어받습니다. |
| INFO | `no checkpoint in B, full sync` | 체크포인트가 없어 풀 싱크를 합니다(첫 실행 등). |
| WARN | `checkpoint in B is not usable, full sync: state=SYNCING` | 이전 풀 싱크가 끝나기 전에 종료됐습니다. 처음부터 다시 합니다. |
| INFO | `source selected: host=… mode=resume\|full_sync` | 소스 replica를 골랐습니다. `resume`은 증분, `full_sync`는 풀 싱크입니다. |
| WARN | `target not ready, retrying: stage=… cause=…` | Site B에 연결하지 못했습니다. 1초마다 재시도하고, 원인이 바뀔 때만 로그를 남깁니다. |
| ERROR | `cannot write to B, closing the session and retrying from the checkpoint stored in B: cause=…` | Site B에 쓰지 못했습니다(연결 끊김, `READONLY`, `OOM` 등). **프로세스는 종료되지 않습니다.** 정확한 원인(`cause`)을 한 번 남기고, 세션을 닫은 뒤 Site B에 저장된 체크포인트부터 다시 시도합니다. 같은 원인은 반복해서 남기지 않으며 `GET /api/v1/status`에 `WAITING_TARGET`과 `detail`로 나옵니다. |
| WARN | `B has not accepted writes for 2m10s: cannot write to B: …` | 위 ERROR가 이어지는 동안 `wait-log-ms`(기본 20초)마다 지속 시간과 현재 원인을 반복해서 남깁니다. 원인이 바뀌면 `reconnecting:` 뒤에 함께 나옵니다. |
| INFO | `writing to B works again` | Site B에 다시 쓸 수 있게 되어 이어서 복제합니다. |
| WARN | `cluster not reachable, retrying: cause=…` | (Cluster) Site A 또는 Site B 클러스터에 연결하지 못했습니다. 1초마다 재시도합니다. |
| WARN | `waiting for source: …` | master가 없거나(`no online master (failover in progress)`), replica가 없거나(`no online replica yet, waiting`), 있어도 쓸 수 없습니다(`no usable replica: <ip> <사유>`, 사유 목록은 [문제 해결](#문제-해결) 참고). 0.5초마다 다시 확인하고, 사유가 바뀔 때만 로그를 남깁니다. |
| WARN | `still waiting for source for 2m10s: no online replica yet, waiting` | 위 대기가 이어지는 동안 `wait-log-ms`(기본 20초)마다 지속 시간과 현재 사유를 반복해서 남깁니다. 소스를 찾으면 멈춥니다. |
| WARN | `source session ended, reselecting: host=… cause=…` | 소스 replica와 연결이 끊겼습니다. 다른 replica를 찾습니다. |
| INFO | `source became master, switching: host=…` | 소스 replica가 master로 승격되어 다른 replica로 옮깁니다. |
| INFO | `checkpoint re-anchored after failover: replid=a -> b …` | 승격 후 같은 위치를 새 master의 이력 id로 맞췄습니다. 증분으로 이어집니다. |
| WARN | `no replica can serve the checkpoint, dropping it and full sync (B will be flushed)` | 폴백이 발동해 풀 싱크로 전환합니다. |
| WARN | `source history diverged, dropping checkpoint and full sync (B will be flushed)` | Site A의 이력이 갈라졌습니다(강제 failover 등). 풀 싱크로 전환합니다. |
| ERROR | `fatal, shutting down: …` | 복구할 수 없는 오류입니다(Site B 쓰기 실패는 해당하지 않습니다). 프로세스가 종료됩니다. |
| WARN | `operator request: action=… partition=… from=<IP>` | 운영자 API로 변경 요청(`pause`, `resume`, `full-sync`)이 들어왔습니다. 호출한 IP가 남습니다. |
| WARN | `paused by the operator: force=…` / INFO `resumed by the operator: …` | 운영자가 중지하거나 재개했습니다. |
| WARN | `full sync requested by the operator: checkpoint deleted, B will be flushed when the snapshot starts` | 운영자가 풀 싱크를 요청했습니다. |
| INFO | `paused by the operator (flag stored in B), waiting for resume` | 시작할 때 Site B에 중지 표시가 있어 중지된 채로 시작했습니다. |

### 종료 코드

| 코드 | 의미 |
|---|---|
| `1` | 치명 오류(Site A 이벤트를 해석·적용하지 못함, 소스 Redis가 7 미만이라 명령 목록을 못 가져옴, Cluster 모드에서 db 0이 아닌 데이터 등). Site B에 쓰지 못하는 것은 종료 사유가 아닙니다. |
| `2` | (Cluster) Site A와 Site B의 shard 수 또는 slot 범위가 다릅니다. 시작할 때 또는 운영 중 resharding으로 달라진 경우이며 **자동 복구하지 않습니다.** |

종료되면 컨테이너/프로세스 관리자가 재시작합니다. 코드 `2`는 재시작해도 레이아웃이 같아지기 전에는 반복해서 종료됩니다.

## 운영자 API

실행 중인 복제의 진행 현황을 보고, 복제를 멈추거나 다시 시작하고, 풀 싱크를 강제하는 HTTP API입니다. `tool` 프로파일에서는 뜨지 않습니다.

- Swagger UI: `http://localhost:8081/swagger-ui.html`, OpenAPI 문서: `/v3/api-docs`
- 오류는 [RFC 9457](https://www.rfc-editor.org/rfc/rfc9457) problem detail(JSON)이고, 입력 검증 오류는 `errors[].field`/`message`로 항목마다 알려줍니다.

> **인증이 없습니다.** `full-sync`는 Site B를 비웁니다. 그래서 기본값은 `server.address=127.0.0.1`이고, 컨테이너 안에서 `curl`을 쓰거나 `kubectl port-forward pod/<redis-dr> 8081:8081`로 접근합니다. `SERVER_ADDRESS=0.0.0.0`처럼 다른 주소로 열면 그 네트워크에 닿는 누구나 호출할 수 있으므로, NetworkPolicy 등으로 접근을 직접 제한해야 합니다. 변경 요청(`pause`, `resume`, `full-sync`)은 호출한 IP와 함께 WARN 로그(`operator request: …`)로 남습니다.

| 메서드 | 경로 | 설명 |
|---|---|---|
| GET | `/api/v1/status` | 모든 partition의 상태 |
| GET | `/api/v1/partitions/{index}` | partition 하나의 상태 |
| GET | `/api/v1/partitions/{index}/resume-check` | `resume`이 체크포인트에서 이어받을 수 있는지(아무것도 바꾸지 않음) |
| POST | `/api/v1/pause?partition=&force=false` | 복제 중지 |
| POST | `/api/v1/resume?partition=` | 체크포인트에서 재개 |
| POST | `/api/v1/full-sync?partition=` | 체크포인트를 지우고 처음부터 풀 싱크. body `{"confirm":"FLUSH_B"}` 필수 |

`partition`은 partition 번호입니다(Sentinel 모드는 `0`, Cluster 모드는 shard 순서). 생략하면 모든 partition에 적용합니다. 여러 partition에 적용할 때는 **전부 검사한 뒤에** 변경하므로, 하나라도 거부되면 아무것도 바뀌지 않습니다.

```bash
curl -s localhost:8081/api/v1/status
curl -s -X POST localhost:8081/api/v1/pause
curl -s localhost:8081/api/v1/partitions/0/resume-check
curl -s -X POST localhost:8081/api/v1/resume
curl -s -X POST localhost:8081/api/v1/full-sync -H 'Content-Type: application/json' -d '{"confirm":"FLUSH_B"}'
```

### 상태 읽는 법

`state`는 `INITIALIZING`(Site B 연결·체크포인트 읽는 중), `WAITING_SOURCE`(`detail`에 사유), `WAITING_TARGET`(Site B에 쓰지 못해 재시도 중, `detail`에 원인), `FULL_SYNC`, `STREAMING`, `PAUSED` 중 하나입니다.

| 필드 | 의미 |
|---|---|
| `sourceHost`, `sessionMode`, `sessionStartedAt` | 읽고 있는 Site A replica, `resume` 또는 `full_sync`, 세션 시작 시각 |
| `offset` | Site A에서 읽은 위치. 세션 중에는 실시간 값 |
| `persistedState`, `persistedOffset` | Site B에 저장된 체크포인트 |
| `masterOffset`, `lagBytes` | Site A master의 offset과 `masterOffset - offset`(바이트). 풀 싱크 중에는 `null` |
| `applied`, `appliedPerSecond` | Site B에 반영한 명령 수와 최근 처리량 |
| `restored`, `expectedKeys`, `approxPercent` | 풀 싱크로 복원한 키 수, 시작 시점 source replica의 키 수, 그 비율(**근사값**) |
| `asOf` | `persisted*`, `masterOffset`, `lagBytes`, `appliedPerSecond`를 잰 시각 |

- `persisted*`, `masterOffset`, `lagBytes`, `appliedPerSecond`는 `stat-report-ms`마다 갱신되므로, 상태가 바뀐 직후(예: 풀 싱크가 끝나 `STREAMING`이 된 순간)에는 최대 그 주기 동안 이전 값(`persistedState: SYNCING` 등)이 보일 수 있습니다. `asOf`로 측정 시각을 확인하세요.
- `status`는 Site A/B를 호출하지 않습니다. 메모리 값과 `stat-report-ms`마다 갱신한 값을 보여주므로, B나 A에 닿지 않을 때도 응답하고 `detail`에 오류가 표시됩니다.
- 지연은 **바이트 단위**입니다. 초 단위 지연은 제공하지 않습니다.
- `approxPercent`는 source replica의 키 수와 비교한 추정치이고, TTL 만료나 진행 중 쓰기 때문에 맞지 않을 수 있으며 100을 넘으면 100으로 표시합니다. 정확한 완료 여부는 `full sync done` 로그나 `state`가 `STREAMING`으로 바뀌는 것으로 판단하세요.
- 시작 직후 Site A/B가 응답하기 전에는 `engine`이 `STARTING`이고 `partitions`가 비어 있습니다. 이때 변경 요청과 partition 조회는 `503`입니다.

### pause / resume / full-sync

- **`pause`**: 세션을 닫고 체크포인트를 그대로 둡니다. 중지 상태는 **Site B에 저장**되어(`{dr}:paused`, Cluster 모드는 `{<tag>}:dr:paused:shard-<N>`) redis-dr을 재시작해도 중지된 채로 시작합니다. 이미 중지 상태면 아무것도 하지 않습니다. Site B에 저장하지 못하면 `503`이고 중지되지 않습니다.
  - **풀 싱크가 진행 중이면** `409`입니다. 이미 B가 비워져 있고 스냅샷은 이어받을 수 없어, 재개하면 처음부터 다시 해야 합니다. 그래도 멈추려면 `force=true`를 줍니다.
  - 중지하는 동안 지연은 계속 커집니다. 중지 시간이 Site A replica의 복제 backlog를 넘으면 체크포인트를 쓸 수 없게 되어 `resume`이 거부됩니다.
- **`resume`**: Site B에 저장된 체크포인트에서 이어받습니다. **스스로 풀 싱크를 시작하지 않습니다.** 체크포인트가 없거나(`SYNCING` 포함), Site A 이력이 갈라졌거나, 어떤 replica도 그 위치를 서빙할 수 없으면 `409`로 거부하고 아무것도 바꾸지 않습니다. 풀 싱크가 필요하면 `full-sync`를 호출하세요. 중지 상태가 아니면 아무것도 하지 않습니다. 먼저 `resume-check`로 확인할 수 있습니다.
- **`full-sync`**: 중지 → Site B의 체크포인트 삭제 → 다시 실행을 한 번에 합니다. 스냅샷이 시작되면 **Site B(Cluster 모드는 해당 shard)가 비워지고** 다 로드될 때까지 DR을 쓸 수 없습니다. 중지 상태였더라도 호출하면 다시 실행 상태가 됩니다. 아래 [수동으로 풀 싱크 시키기](#수동으로-풀-싱크-시키기)를 대체합니다. 요청 직후 바로 시작하지 않을 수 있습니다. Site A master의 `repl-diskless-sync-delay`(기본 5초)만큼 스냅샷 시작이 늦어집니다.

Site B에 저장되는 관리용 키(체크포인트, 중지 표시)는 복제 대상 데이터가 아니므로 Site A와 Site B의 데이터 건수를 비교할 때 제외하세요.

## 장애 시 동작

| 상황 | 동작 |
|---|---|
| redis-dr 재시작 | Site B의 체크포인트에서 이어받습니다(풀 싱크 없음). |
| 소스 replica 장애 | 다른 replica로 즉시 옮겨 이어받습니다. |
| Site A master 장애 후 승격 | 체크포인트를 새 master 기준으로 맞추고(`re-anchored`) 이어받습니다. 남은 replica가 합류할 때까지 `waiting for source`로 대기할 수 있습니다. |
| 소스 replica가 master로 승격 | 감지해서 다른 replica로 옮깁니다. |
| Site A의 replica가 전부 죽음 | 종료하지 않고 체크포인트를 유지한 채 대기합니다. `source session ended` 뒤에 `waiting for source: no usable replica: <ip> unreachable`(Sentinel/클러스터가 아직 내려갔다고 표시하지 않은 동안), 표시한 뒤에는 `no online replica yet, waiting`이 나오고, 이후 `wait-log-ms`마다 `still waiting for source for …`가 반복됩니다. `GET /api/v1/status`는 `WAITING_SOURCE`와 같은 사유를 보여줍니다. 그 동안 Site B에는 새 쓰기가 반영되지 않아 DR 지연이 커집니다. replica가 돌아오면 이어받습니다. replica가 없는 시간은 풀 싱크 폴백 시간에 포함되지 않으므로 Site B가 비워지지 않습니다. |
| Site A의 이력이 갈라짐 | 자동으로 체크포인트를 버리고 풀 싱크를 합니다. |
| Site B master 장애, 또는 Site B가 쓰기를 거부(`READONLY`, `OOM` 등) | 종료하지 않고 대기합니다. 쓰기 실패 원인을 ERROR로 한 번 남기고 `state`를 `WAITING_TARGET`으로 바꾼 뒤, 1초마다 Site B(Sentinel/클러스터에서 새 master 주소를 다시 받음)를 확인합니다. Site B가 다시 쓸 수 있게 되면 **Site B에 저장된 체크포인트부터** 이어서 복제하고(`writing to B works again`), 데이터와 체크포인트가 한 스크립트로 함께 기록되므로 유실이나 중복이 없습니다. 풀 싱크 도중이었다면 스냅샷은 이어받을 수 없으므로 처음부터 다시 합니다. 이 동안 Site B에 쓰는 pause/full-sync API는 `503`입니다. |

### 풀 싱크 폴백

살아 있는 replica가 있지만 **전부** 체크포인트를 서빙할 수 없을 때(뒤처졌거나, 방금 풀 싱크로 합류해 백로그가 체크포인트 이후부터 시작하거나, 이력이 다를 때), redis-dr은 `fullsync-fallback-seconds` 동안 계속 후보를 다시 확인합니다. 그 시간 내내 서빙 가능한 replica가 없으면 체크포인트를 버리고 풀 싱크로 전환합니다. 이때 Site B(Cluster 모드는 해당 shard)가 비워지고, 스냅샷이 다 로드될 때까지 그 범위의 DR은 사용할 수 없습니다.

- replica가 평소 따라잡는 데 걸리는 시간보다 충분히 크게 잡으세요. 뒤처진 상태도 "서빙 불가"로 세기 때문에, 값이 작으면 잠깐 뒤처진 것만으로 Site B가 비워질 수 있습니다.
- `0`이면 폴백을 쓰지 않습니다. 그 경우 위 조건에 걸리면 사용 가능한 replica가 생길 때까지 **계속 대기**하며, 스스로 풀리지 않을 수 있습니다.
- replica가 없거나 연결이 안 되는 시간은 폴백 시간에 포함되지 않습니다.

### 수동으로 풀 싱크 시키기

[운영자 API](#운영자-api)의 `POST /api/v1/full-sync`가 아래 절차를 대신합니다. API를 쓸 수 없을 때를 위한 수동 방법입니다.

Site B에 저장된 체크포인트 키를 지우고 redis-dr을 다시 시작하면 처음부터 풀 싱크를 합니다. **Site B가 비워집니다.** 아래 순서를 지키세요.

1. **redis-dr을 먼저 중지합니다.** 실행 중이면 redis-dr이 명령을 적용할 때마다 체크포인트 키를 다시 만들기 때문에, 지워도 소용이 없습니다.
2. Site B master에서 체크포인트 키를 지웁니다.
3. redis-dr을 시작합니다. `no checkpoint in B, full sync` 로그가 나오면 풀 싱크가 시작된 것입니다.

**Sentinel 모드**: 키는 Site B master의 db 0에 있는 `{dr}:checkpoint` 하나입니다. Site B master 주소는 Site B의 Sentinel에 물어봅니다.

```bash
# B master 주소 확인
redis-cli -h sentinel-b -p 26379 SENTINEL get-master-addr-by-name mymaster

# 지우기 전에 내용 확인(state, replid, offset), 그다음 삭제
redis-cli -h <B master> HGETALL '{dr}:checkpoint'
redis-cli -h <B master> DEL '{dr}:checkpoint'
```

**Cluster 모드**: 키는 shard마다 하나이고 해당 shard의 Site B master에 있습니다. 이름은 `{<tag>}:dr:checkpoint:shard-<N>`이며 앞부분의 hash tag가 shard마다 달라서 찾아야 합니다. 한 shard만 풀 싱크하려면 그 shard의 키만 지우면 되고, 나머지 shard는 계속 복제합니다.

```bash
# B master 목록 확인
redis-cli -h <B 클러스터의 아무 노드> CLUSTER NODES | grep master

# 각 B master에서 키 찾기 (SCAN은 노드 단위이므로 master마다 실행)
redis-cli -h <B master> --scan --pattern '*:dr:checkpoint:shard-*'

# 찾은 키를 그 master에서 삭제 (키 이름의 중괄호가 셸에서 풀리지 않게 따옴표로 감쌉니다)
redis-cli -h <B master> DEL '{s0x123}:dr:checkpoint:shard-0'
```

컨테이너나 Pod 안에서 실행한다면 `docker exec <B master 컨테이너> redis-cli …` 또는 `kubectl exec <B master Pod> -- redis-cli …` 형태로 같은 명령을 실행합니다.

`pause`로 중지했다면 같은 곳에 중지 표시 키(`{dr}:paused`, Cluster 모드는 `{<tag>}:dr:paused:shard-<N>`)가 있습니다. 풀 싱크 전에 `DEL`로 지워야 redis-dr이 시작합니다.

체크포인트는 `state`(`OK`/`SYNCING`), `replid`, `offset` 필드를 가진 hash입니다. `DEL` 대신 `HSET <키> state SYNCING`으로 바꿔도 같은 효과이고, 이 경우 시작할 때 `checkpoint in B is not usable, full sync: state=SYNCING`이 출력됩니다. 이 키는 복제 대상 데이터가 아니라 redis-dr의 관리용 키입니다.

## 테스트 도구 (Cluster 모드)

두 클러스터에 데이터를 넣고 Site A와 Site B를 비교하는 수동 도구입니다. `tool` 프로파일로 실행하면 **복제는 시작하지 않습니다.**

```bash
redis-dr --spring.profiles.active=tool load   <prefix> <n>        # n개 × (string/hash/set/zset/list)를 A에 넣음
redis-dr --spring.profiles.active=tool write  <prefix> <seconds>  # A에 SET을 계속 쓰고 확인된 쓰기 수를 출력
redis-dr --spring.profiles.active=tool verify                     # A의 모든 키를 B와 비교(타입, 값, TTL)하고 B의 여분 키를 찾음
```

- `REDIS_DR_MODE=cluster`와 `REDIS_DR_CLUSTER_SOURCE_NODES`/`TARGET_NODES`가 필요합니다.
- `verify`는 모두 일치하면 종료 코드 `0`, 다르면 `1`입니다. 개수는 비교하지 않습니다. TTL이 있는 키가 검사 중에도 만료되기 때문이며, 2분 이내에 만료되는 키는 건너뜁니다. Site B의 체크포인트·중지 표시 키는 제외합니다.
- 인자 없이 실행하면 사용법을 출력합니다.

## 제약 사항

- **한 Site A → Site B 쌍에 redis-dr 인스턴스 하나**를 실행합니다.
- 운영자 API에는 인증이 없습니다. 기본은 loopback에서만 듣고, 다른 주소로 열면 접근 제한은 직접 해야 합니다.
- 소스는 항상 replica입니다. master에서 읽지 않습니다.
- `MULTI`/`EXEC`는 원자적으로 적용하지 않고 명령 단위로 적용합니다.
- Cluster 모드는 Site A와 Site B의 shard 수와 slot 범위가 같아야 합니다. resharding이 감지되면 정지합니다.
- Cluster 모드는 db 0만 지원합니다.
- Site A의 `PUBLISH __sentinel__:*`는 Site B의 Sentinel이 Site A의 Sentinel을 피어로 인식하지 않도록 Site B에 적용하지 않습니다.
- 비동기 복제의 한계로, Site A에서 failover가 일어나면 옛 master가 그 사이 받은 쓰기는 사라집니다. redis-dr이 갈라짐을 감지하면 풀 싱크로 Site B를 Site A에 맞춥니다.

## 문제 해결

| 증상 | 확인할 것 |
|---|---|
| 시작 직후 `expected host:port but got '…'` 오류로 종료 | `…nodes` 값이 `host:port` 형식인지 확인하세요. 포트가 빠졌거나 목록이 비어 있어도 같은 오류가 납니다. |
| `target not ready, retrying`이 계속 나옴 | Site B의 Sentinel/클러스터 주소와 Site B master 접속 가능 여부를 확인하세요. |
| `GET /api/v1/status`가 `WAITING_TARGET` | `detail`의 원인을 보세요. `ConnectException`이면 Site B master가 내려갔거나 failover 중이고, `READONLY`면 master가 replica로 강등됐고, `OOM`이면 Site B의 `maxmemory`를 확인합니다. 원인이 풀리면 자동으로 이어집니다. |
| Cluster 모드에서 `exit 2`로 반복 종료 | 로그의 `shard count differs` 또는 `slot layout differs` 내용을 보고 Site A와 Site B의 `CLUSTER SHARDS`를 맞추세요. |
| `source selected`가 나오지 않고 `waiting for source`만 반복 | Site A에 online replica가 있는지, master가 있는지 확인하세요. |
| `waiting for source: no usable replica: <ip> <사유>`가 계속됨 | replica가 목록에는 있지만 쓸 수 없는 상태입니다. 사유는 `unreachable`(내려갔지만 Sentinel/클러스터가 아직 내려갔다고 표시하지 않음), `link down`, `role=master`, `not following current master`, `backlog starts after the checkpoint`, `behind`, `replid mismatch` 중 하나입니다. 같은 사유는 한 번만 로그에 남고 `GET /api/v1/status`의 `detail`에도 나옵니다. `unreachable`은 Sentinel/클러스터가 노드를 내려갔다고 표시하면 `no online replica yet, waiting`으로 바뀝니다. 마지막 세 가지는 체크포인트를 서빙할 수 없는 경우이므로 `INFO replication`의 `master_repl_offset`, `repl_backlog_first_byte_offset`, `master_link_status`를 확인하고, 폴백이 꺼져 있다면(`0`) 값을 설정하거나 [수동 풀 싱크](#수동으로-풀-싱크-시키기)(또는 `full-sync` API)를 하세요. 운영자가 중지한 상태(`state`가 `PAUSED`)이면 이 로그가 나오지 않습니다. |
| `cannot list the source's commands (Redis 7+ required)`로 종료 | Site A의 Redis가 7 미만입니다. |
| `resume`이 `409` | 응답의 사유를 보세요. 체크포인트가 없거나 `SYNCING`이면 이어받을 수 없고, 중지 시간이 길어 replica의 backlog를 지났거나 Site A 이력이 갈라진 경우에도 거부합니다. `GET /api/v1/partitions/{index}/resume-check`로 먼저 확인하고, Site B를 다시 채워도 되면 `full-sync`를 호출하세요. |
| 운영자 API에 접속이 안 됨 | 기본은 `127.0.0.1`에서만 듣습니다. `kubectl port-forward`로 접근하거나, Docker에서는 `SERVER_ADDRESS=0.0.0.0`과 `-p 127.0.0.1:8081:8081`을 쓰세요. 프로세스가 종료됐다면(`fatal, shutting down`) API도 없습니다. |
| `stat:`의 `offset`은 늘어나는데 `cp=`가 한참 전 값 | 정상입니다. 체크포인트는 명령을 Site B에 적용할 때 갱신되므로 쓰기가 없는 동안에는 `offset`만 증가합니다. |
| `redis-replicator` 로그(`PING`, `PONG`, `REPLCONF` …)가 너무 많음 | `LOGGING_LEVEL_COM_MOILIONCIRCLE=WARN`으로 줄입니다. |

| 쓰기가 없을 때 `[redis-replicator] socket error … EOFException`과 `source session ended`가 주기적으로 반복됨 | 읽기 타임아웃과 Site A의 PING 주기가 가까운 경우입니다. 로그 첫 줄의 `EOFException`은 라이브러리가 원래 예외(타임아웃)를 가린 것이고, `reconnecting … retry times:1`도 라이브러리가 실제로 재연결하는 것이 아니라 엔진이 소스를 다시 골라 이어받는 것입니다(데이터 유실 없음). `redis-dr.replication.read-timeout-seconds`가 `source-ping-period-seconds`의 2배보다 큰지, 그리고 `source-ping-period-seconds`가 A의 `repl-ping-replica-period`와 같은지 확인하세요. 시작할 때 `replication read timeout: …` 로그로 적용된 값을 볼 수 있습니다. |
## 개발

```bash
./gradlew test          # 단위 테스트와 설정/기동 테스트
```

- 패키지 구성: `config`(설정), `model`(값 객체), `client`(Redis 접근), `service`(인터페이스)와 `service/impl/{sentinel,cluster}`(구현), `controller`(운영자 API), `scheduler`(주기 작업), `runner`(테스트 도구), `exception`, `log`, `util`
- 복제 로직은 하나(`DrEngine`)이고, 모드별 차이는 Site A 조회(`SourceTopology`)와 Site B 쓰기(`TargetWriter`) 구현에만 있습니다.
- 소스 선택과 체크포인트 보정 규칙은 `SourceSelectorImpl`에 있으며 단위 테스트로 검증합니다.
- 운영자 API는 `OperatorController` → `OperatorService` → `ReplicationWorker`(`status`, `pause`, `resume`, `fullSync`)로 이어집니다. 중지 표시와 체크포인트 삭제는 `TargetWriter`가 Site B에 합니다.
- Site B 쓰기 실패(`IOException`)는 `ReplicationWorkerImpl.onEvent`에서 처리해 세션을 닫고 B의 체크포인트부터 다시 시작합니다. 그 외 예외만 치명 오류입니다.
