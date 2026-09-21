# 302 · Spark lab

Two refereed rounds for **Monday 21 September 2026**, session 4 of the data
pipelines block of HES-SO module *302 Data Computation*.

Round 2 puts Spark on the Kubernetes cluster built in round 1. Round 3 counts
words on it, two ways, and then makes the same count slow on purpose.

Nothing is installed. Everything runs inside the official Spark image, which is
published for both `linux/amd64` and `linux/arm64`, so an Apple Silicon laptop
needs no emulation.

The corpus is **not** in this repository. 18 846 small text files made a 17.6 MiB
pack for 34 MB of text, and every clone paid for it; it is distributed as a zip
through ISC Learn instead.

## Getting set up

Before the session, download **`20news.zip`** (20 MB) from ISC Learn. Nothing else
is needed in advance: round 2 clones nothing, and round 3 clones this repository
in one second because the corpus is not in it.

---

## Round 2 · deploy Spark onto the cluster

Round 1 left a k3s cluster running in a container named `k8s`. A container sees
only the folders it was handed when it was created, and there is no command that
adds one afterwards. So the cluster is made again, this time with an empty `lab`
folder attached that round 3 will fill.

**`mkdir lab` first.** If it does not exist, Docker creates it owned by `root`
and nothing you write into it later will be permitted.

```bash
mkdir lab
docker rm -f k8s
docker run -d --name k8s --privileged --tmpfs /run --tmpfs /var/run \
  -p 6443:6443 -v "$PWD/kube:/output" -v "$PWD/lab:/lab" \
  rancher/k3s:v1.36.4-k3s1 server --disable=traefik \
  --write-kubeconfig /output/config --write-kubeconfig-mode 644

# a kubectl, for anyone who has not installed one. Unchanged from round 1.
docker run --rm -it --network container:k8s \
  -v "$PWD/kube:/kube:ro" -e KUBECONFIG=/kube/config \
  --entrypoint sh alpine/kubectl
```

Spark's own cluster is a master that hands out cores and workers that supply
them, plus a name the workers can dial. That is one Service and two Deployments,
created by hand, in this order: the master looks its own name up when it starts.

```bash
kubectl create service clusterip spark-master --clusterip="None" --tcp=7077:7077

kubectl create deployment spark-master --image=spark:3.5.4-python3 \
  -- /opt/spark/bin/spark-class org.apache.spark.deploy.master.Master \
     --host spark-master

kubectl create deployment spark-worker --image=spark:3.5.4-python3 --replicas=2 \
  -- /opt/spark/bin/spark-class org.apache.spark.deploy.worker.Worker \
     spark://spark-master:7077 --cores 1 --memory 1g

kubectl get pods
kubectl logs deploy/spark-master | grep Master:
```

The first pod takes one to three minutes to start, because the cluster downloads
the 535 MB image itself: it cannot see your laptop's copy, and it would not use
it if it could.

**Done when** the master's log names every worker, with the cores each one
offered, and your referee can say why the cluster downloaded an image the laptop
may well already have had.

Three things to write on your sheet:

1. `docker image ls` on the laptop and `crictl images` inside the cluster list
   the same image under the same name. Why did the cluster download its own copy?
2. `replicas: 2` and the master's count of workers are two different numbers that
   happen to agree. Which would be wrong first if a worker pod were deleted?
3. The Service has `clusterIP: None`. What would break if it had an address of
   its own?

**Extension.** Run the example job that ships in the image, delete a worker pod
and watch both the controller and the master react, then hand the cluster an
image instead of letting it download one.

```bash
kubectl exec deploy/spark-master -- /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --conf spark.driver.host=spark-master \
  --class org.apache.spark.examples.SparkPi \
  local:///opt/spark/examples/jars/spark-examples_2.12-3.5.4.jar 100

docker save spark:3.5.4-python3 | docker exec -i k8s ctr -n k8s.io images import -
```

---

## Round 3 · count the words with DataFrames, then with RDDs

First fill the folder round 2 attached, then hand the cluster a file that does the
two things `kubectl create` could not: give the pods that folder, and give the
master a name in cluster DNS.

```bash
git clone https://github.com/oesteban/isc-302-2026-2027-spark-lab lab
unzip ~/Downloads/20news.zip -d lab/data
ls lab/data/20news | wc -l                                        # 18846

kubectl apply -f https://raw.githubusercontent.com/oesteban/isc-302-2026-2027-spark-lab/main/spark-standalone.yaml
kubectl exec deploy/spark-worker -- ls /lab/data/20news | wc -l    # 18846, from inside
kubectl scale deployment spark-worker --replicas=4
```

It warns that the objects were made by hand and it has nothing to compare
against. That is true, harmless, and it patches them anyway. From here on
`--conf spark.driver.host` is not needed.

Two scripts. Same data, same answer, different machinery. Both are submitted to
the cluster round 2 built, so the partition counts and the clocks below are
properties of that cluster, not of your laptop.

```bash
kubectl scale deployment spark-worker --replicas=4

kubectl exec deploy/spark-master -- /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 /lab/wordcount_df.py /lab/data

kubectl exec deploy/spark-master -- /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 /lab/wordcount_rdd.py /lab/data
```

The second one takes minutes. Start it, then read the first one's plan while it
runs: its partition count is printed in the first seconds, at the head of the
lineage, long before the job ends.

**Done when** your referee can point at the line of the DataFrame plan where the
shuffle happens, and say what the two `Exchange` rows are for.

Three things to write on your sheet:

1. `explain()` prints a complete plan **before any data is read**. Which line of
   the script actually caused work to happen?
2. The two scripts report a different number of **input partitions**. Why would
   the same 18 846 files be divided differently by the two APIs?
3. They take different times. Which is faster, and is the difference explained
   by the partition count?

**Extension · make the same count slow on purpose.** `slowdown.py` runs the count
three times over the same 34 MB, at 8, 800 and 8000 partitions. Run it on four
workers, then on one, for six wall clocks.

```bash
kubectl exec deploy/spark-master -- /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 /lab/slowdown.py /lab/data

kubectl scale deployment spark-worker --replicas=1
# ...the same submission again, then put it back to 4
```

**Done when** the group can say in one sentence why 8000 partitions over the same
34 MB is slower than 8, on both cluster sizes.

Your numbers will not match anyone else's, because the cluster runs on whatever
cores Docker gives it. The **shape** will match everyone's.

---

## What is in here

| path | what |
|---|---|
| `spark-standalone.yaml` | the round 2 objects plus the volume and the hostname; applied in round 3 |
| `data/20news/` | **not in git**: unzip `20news.zip` from ISC Learn to here |
| `data/stopwords.txt` | 203 stopwords: ordinary English, plus the mail header field names |
| `wordcount_df.py` | the count with the DataFrame API, and `explain()` |
| `wordcount_rdd.py` | the same count with RDDs, and `toDebugString()` |
| `slowdown.py` | the partition and parallelism experiment |

`make check` runs both counts **on your laptop**, without the cluster, and asserts
they agree. It needs `data/20news` to be unzipped first. It takes several minutes,
because the RDD version is slow on purpose (see round 3, question 3). The `df`,
`rdd`, `slow` and `slow1` targets are the same local shortcuts, and exist so that
the lab can be prepared without a cluster.

### About the corpus

The *20 Newsgroups* collection is a public dataset of Usenet posts from 1993,
distributed from <http://qwone.com/~jason/20Newsgroups/>. `20news.zip` on ISC
Learn carries the whole `bydate` split, train and test, 18 846 messages and
34 MB, with **every email address deleted**. The text is otherwise unmodified,
headers included, which is why `university` finishes fifth: it is mostly the
`Organization:` line, not people talking about universities.

Tokens shorter than three characters are dropped, because splitting on `\W+`
turns `don't` into `don` and `t`, and without the filter the top of the list is
apostrophe debris.
