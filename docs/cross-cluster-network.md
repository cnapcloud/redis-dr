# 클러스터 간 네트워크 구성

Site A와 Site B가 서로 다른 Kubernetes 클러스터(또는 다른 네트워크)에 있을 때, redis-dr이 양쪽 Redis에 접속하려면 무엇을 열어야 하는지 정리한 문서입니다. 설정 항목 자체는 [README의 설정](../README.md#설정)을 참고하세요.

> 이 문서는 코드 동작을 기준으로 정리한 것입니다. README의 Kubernetes 시험은 A와 B를 한 클러스터에 두고 했고, 서로 다른 클러스터에서 시험한 것은 아닙니다.

## 왜 필요한가

설정 기본값과 README 예시의 `redis-b-leader.redis-dr-b:6379`, `sentinel-b.redis-dr-b:26379` 같은 주소는 Kubernetes 내부 DNS 이름(`서비스.네임스페이스`)입니다. 같은 클러스터 안에서만 풀립니다. A와 B가 다른 클러스터이면 redis-dr이 있는 쪽에서 반대편 이름을 풀 수 없으므로, 반대편에서 실제로 닿는 주소를 써야 합니다.

## redis-dr이 접속하는 곳

| | Sentinel 모드 | Cluster 모드 |
|---|---|---|
| Site A 토폴로지 조회 | Site A Sentinel | Site A의 아무 노드(seed)에 `CLUSTER SHARDS` |
| Site A 복제 스트림(PSYNC) | source replica 1개 | shard마다 source replica 1개 |
| Site B 토폴로지 조회 | Site B Sentinel | Site B의 아무 노드(seed)에 `CLUSTER SHARDS` |
| Site B 쓰기 | master 1개 | shard마다 master 1개 |

설정에 적는 주소는 **조회를 시작하는 seed(Sentinel 또는 클러스터 노드)** 뿐입니다. 그다음 복제와 쓰기에 쓰는 노드 주소는 seed가 응답으로 알려 준 주소입니다. 그래서 seed만 열어서는 충분하지 않고, 응답에 들어 있는 주소에도 모두 접속할 수 있어야 합니다.

## redis-dr을 어디에 둘 것인가

두 클러스터 사이에서 열어야 하는 지점을 줄이려면 **redis-dr을 Site B(DR) 쪽 클러스터에 두는 구성**을 권합니다.

- Site B 쓰기는 같은 클러스터 안의 내부 주소로 해결됩니다. Site B를 밖으로 노출할 필요가 없습니다.
- 클러스터 사이로는 Site A 쪽 연결만 남습니다. Sentinel 모드는 Site A Sentinel과 source replica, Cluster 모드는 seed와 shard별 source replica입니다.
- DR 사이트가 Site A의 장애와 무관하게 독립적으로 동작합니다.

반대로 redis-dr을 Site A 쪽에 두면 Site B의 master(Cluster 모드는 shard별 master 전부)를 노출해야 합니다. 아래 설명은 노출해야 하는 쪽 기준이며, 어느 쪽이든 같은 방식을 적용하면 됩니다.

## Cluster 모드: master마다 개별 주소가 필요합니다

Redis Cluster는 키마다 담당 master가 정해져 있습니다(`CRC16(키) mod 16384`로 구한 slot의 소유자). redis-dr은 Site A shard *i*의 replica 스트림을 Site B shard *i*의 master에 씁니다. 한 shard의 키는 그 shard의 master만 받을 수 있고, 체크포인트도 같은 노드에 같은 Lua 스크립트로 써야 합니다. 따라서 redis-dr은 **shard 수만큼의 master에 각각 직접 접속**합니다.

그래서 서비스 하나를 LoadBalancer로 열고 그 앞에서 아무 master로 보내는 방식은 쓸 수 없습니다. 요청이 담당이 아닌 master로 가면 `MOVED`로 거절됩니다. 노드마다 따로 닿는 주소(노드별 LoadBalancer, NodePort, 또는 네트워크 연결 후의 Pod IP)가 있어야 합니다.

### announce 주소

각 노드는 `CLUSTER SHARDS` 응답에 자기에게 접속할 주소를 적어 보냅니다. 이것이 announce 주소입니다. 아무 설정이 없으면 노드가 아는 내부 주소(Pod IP)를 알립니다.

예를 들어 Site B가 master 3개이고 각각 `203.0.113.11~13`으로 노출되어 있다고 하겠습니다.

announce 설정이 없을 때:

```
CLUSTER SHARDS    (요약)
shard 0  slots 0-5460       master ip=10.244.1.15  port=6379
shard 1  slots 5461-10922   master ip=10.244.2.21  port=6379
shard 2  slots 10923-16383  master ip=10.244.3.8   port=6379
```

seed(`203.0.113.11`)로 조회까지는 되지만, 그다음 shard 0에 쓰려고 `10.244.1.15`에 접속하면 이 주소는 Site B 클러스터 안에서만 의미가 있어 실패합니다.

각 노드의 `redis.conf`에 자기가 외부에서 닿는 주소를 지정하면:

```
# Site B shard 0의 노드 (노드마다 값이 다릅니다)
cluster-announce-ip       203.0.113.11
cluster-announce-port     6379
cluster-announce-bus-port 16379
```

```
CLUSTER SHARDS    (요약)
shard 0  slots 0-5460       master ip=203.0.113.11  port=6379
shard 1  slots 5461-10922   master ip=203.0.113.12  port=6379
shard 2  slots 10923-16383  master ip=203.0.113.13  port=6379
```

이제 redis-dr이 응답의 주소로 shard별 master에 직접 접속할 수 있습니다.

주의할 점:

- **`ip`만 지원합니다.** redis-dr은 `CLUSTER SHARDS` 응답의 `ip` 필드로 접속합니다. `cluster-announce-hostname`과 `cluster-preferred-endpoint-type hostname`으로 호스트명을 알려도 redis-dr은 `hostname`/`endpoint` 필드를 읽지 않으므로 소용이 없습니다. Cluster 모드에서는 `cluster-announce-ip`를 쓰세요.
- **seed 주소는 호스트명도 됩니다.** `redis-dr.cluster.*.nodes`에 적는 seed는 접속할 때 이름이 풀리면 됩니다. announce 주소만 IP여야 합니다.
- **replica도 같은 설정을 해야 합니다.** failover가 나면 replica가 master가 되고, redis-dr은 새 master 주소로 옮겨 갑니다. Site A에서는 replica가 source로 쓰이므로 역시 닿아야 합니다.
- **노드마다 값이 다릅니다.** StatefulSet이면 Pod 이름(`redis-b-0`, `redis-b-1`…)에서 값을 만드는 init 스크립트나 operator의 설정 기능으로 넣어야 합니다.
- **노드끼리의 통신도 영향을 받습니다.** announce를 바꾸면 노드들이 서로의 주소도 이 값으로 기억하므로, 클러스터 내부에서도 그 주소(와 bus port)로 닿아야 합니다. 닿지 않으면 클러스터가 불안정해질 수 있어 변경 전에 확인이 필요합니다.
- redis-dr 자신은 cluster bus 포트(기본 `port + 10000`)로 접속하지 않습니다. 노드 간 통신에만 필요합니다.

## Sentinel 모드

Sentinel 모드는 master 하나에만 쓰므로 노출할 지점이 적습니다. redis-dr은 Sentinel에게 master 주소와 replica 목록을 묻고 그 주소로 접속합니다.

- **master 주소:** Sentinel이 `get-master-addr-by-name`으로 알려 주는 주소입니다. Sentinel이 감시하는 주소(`sentinel monitor`에 적은 주소)이므로, 반대편에서 닿는 주소로 감시하게 해야 합니다.
- **replica 주소:** master의 `INFO replication`에 나온 주소입니다. 내부 주소로 나오면 replica의 `replica-announce-ip`, `replica-announce-port`로 닿는 주소를 알리게 합니다.
- **Sentinel 자신:** 필요하면 `sentinel announce-ip`, `sentinel announce-port`를 설정합니다.
- **호스트명으로 알리는 경우:** Sentinel에 `sentinel resolve-hostnames yes`, `sentinel announce-hostnames yes`(Redis 6.2 이상)를 설정하면 호스트명을 쓸 수 있습니다. redis-dr은 Sentinel이 알린 호스트명을 IP로 풀어서 접속하므로, **그 이름이 redis-dr이 있는 곳에서 풀려야 합니다.** 점이 하나인 짧은 이름(`redis-a-0.redis-a`)은 `domain-suffix`를 붙여 풉니다. 이 설정은 Sentinel 모드에만 있습니다.

## 점검

redis-dr이 실제로 실행될 곳(Pod 안이면 `kubectl exec`)에서 확인하세요.

```
# 1. seed가 응답하는지, 응답에 든 주소가 내부 주소가 아닌지 (Cluster 모드)
redis-cli -h <seed 호스트> -p 6379 cluster shards

# 2. 응답에 나온 모든 ip:port에 접속되는지 (master와 replica 전부)
redis-cli -h <응답의 ip> -p <응답의 port> ping

# 3. Sentinel 모드: Sentinel이 알려 주는 주소
redis-cli -h <sentinel 호스트> -p 26379 sentinel get-master-addr-by-name mymaster
redis-cli -h <sentinel 호스트> -p 26379 sentinel replicas mymaster
```

2번에서 하나라도 실패하면 redis-dr도 그 노드에 접속하지 못합니다. 방화벽과 보안 그룹에서는 Redis 포트(기본 6379)와 Sentinel 포트(기본 26379)를 redis-dr이 있는 쪽에서 열어야 합니다.

접속을 못 하는 동안 redis-dr은 종료하지 않고 원인을 로그와 상태 API에 남긴 채 기다립니다([장애 시 동작](../README.md#장애-시-동작)). 주소를 고친 뒤에는 저장된 체크포인트부터 이어서 복제합니다.
