# 302 · Spark lab

Refereed rounds for **Monday 21 September 2026** and **Thursday 24 September
2026**, sessions 4 and 5 of the data pipelines block of HES-SO module
*302 Data Computation*.

On 21.09, round 2 puts Spark on the Kubernetes cluster built in round 1, and
round 3 counts words on it two ways, then makes the same count slow on purpose.

On 24.09, rounds 1 and 2 run the same count over a corpus that arrives as **one
compressed file** rather than 18 846 small ones, and measure what that does to
the plan and to the clock.

Nothing is installed. Everything runs inside the official Spark image, which is
published for both `linux/amd64` and `linux/arm64`, so an Apple Silicon laptop
needs no emulation.

The corpus is **not** in this repository. 18 846 small text files made a 17.6 MiB
pack for 34 MB of text, and every clone paid for it; it is distributed as a zip
through ISC Learn instead.

## Getting set up

Before the 21.09 session, download **`20news.zip`** (20 MB) from ISC Learn.

Before the 24.09 session, download **`simplewiki.txt.bz2`** (76 MB) from ISC
Learn as well, and put it in `lab/data/`. It is 212 354 Simple English Wikipedia
articles, one per line, 259 MB of text once decompressed. It is a derivative of
the 2026-09-01 dump, produced by `wiki_to_text.py` in this repository, so the
step that made it is auditable rather than magic.

**On macOS, check this first.** If your shell forces a platform, every container
here is emulated instruction by instruction: the cluster starts, runs slowly, and
then its runtime stops answering. Measured on an M-series Mac, forced to
`linux/amd64` it died after about half an hour; native, the pods were up in 35 s.

```bash
echo "$DOCKER_DEFAULT_PLATFORM"      # must print an empty line
```

If it prints `linux/amd64`, delete that line from whichever file sets it and
clear it from the running shell as well:

```bash
grep -rn DOCKER_DEFAULT_PLATFORM ~/.zshrc ~/.zprofile ~/.bash_profile ~/.bashrc
unset DOCKER_DEFAULT_PLATFORM
docker rmi -f rancher/k3s:v1.36.4-k3s1 alpine/kubectl
```

All three images this lab uses publish an Apple Silicon build, so nothing needs
emulating. Nothing else
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
  -v k3s-images:/var/lib/rancher/k3s/agent/containerd \
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

**Extension · say the same thing in a file.** Three commands built the cluster and
recorded nothing. Tear them down and apply `spark-as-typed.yaml`, which describes
exactly those three objects and nothing more.

```bash
kubectl delete deployment spark-master spark-worker
kubectl delete service spark-master
kubectl apply -f https://raw.githubusercontent.com/oesteban/isc-302-2026-2027-spark-lab/main/spark-as-typed.yaml
```

It comes back in seconds, because the image never left the cluster's store.

---

## When the cluster stops answering

A laptop that sleeps takes its cluster's container runtime with it. The symptoms
are `kubectl get nodes` reading `NotReady`, commands timing out, or pods stuck in
`ContainerCreating`. Work down this list and stop at the first thing that helps.

```bash
docker restart k8s            # then LEAVE the kubectl shell and start a new one
docker exec k8s crictl ps     # rows mean the runtime is back; a bare header does not
kubectl delete pods --all     # clears pods wedged while it was down
```

Leaving the shell is not optional: the client joined the cluster container's
network namespace, and restarting the cluster replaces it, so an old client
reports `connection refused` for ever afterwards.

If `crictl ps` prints only a header, or pods stay in `ContainerCreating` past a
minute, rebuild: `docker rm -f k8s`, then round 2 again. With the image store on
its named volume that is about 30 seconds. If even that fails, the store itself
is damaged: `docker volume rm k3s-images` and rebuild, accepting the download.

---

## Round 3 · count the words with DataFrames, then with RDDs

Round 3 starts from nothing, so that the file below is demonstrably what built
the cluster, and so that every group is measuring the same thing.

```bash
docker rm -f k8s
rm -rf lab kube && mkdir lab kube          # sudo rm -rf if Docker made one as root
git clone https://github.com/oesteban/isc-302-2026-2027-spark-lab lab
unzip ~/Downloads/20news.zip -d lab/data
ls lab/data/20news | wc -l                 # 18846

# the cluster again: round 2's command, unchanged
docker run -d --name k8s --privileged --tmpfs /run --tmpfs /var/run \
  -p 6443:6443 -v "$PWD/kube:/output" -v "$PWD/lab:/lab" \
  -v k3s-images:/var/lib/rancher/k3s/agent/containerd \
  rancher/k3s:v1.36.4-k3s1 server --disable=traefik \
  --write-kubeconfig /output/config --write-kubeconfig-mode 644

# a client, this time with the manifest file mounted into it
docker run --rm -it --network container:k8s \
  -v "$PWD/kube:/kube:ro" \
  -v "$PWD/lab/spark-standalone.yaml:/spark-standalone.yaml:ro" \
  -e KUBECONFIG=/kube/config --entrypoint sh alpine/kubectl
```

Then, in that shell, read the manifest before applying it. It is the same Service
and two Deployments round 2 typed, plus the two things `kubectl create` has no
option for: a `hostPath` volume so the pods can see `/lab`, and a `hostname` so
cluster DNS answers for the master.

```bash
cat /spark-standalone.yaml
kubectl apply -f /spark-standalone.yaml
kubectl get pods
kubectl scale deployment spark-worker --replicas=4
```

The pods are up in seconds: the image stayed in the named volume when round 2's
cluster was deleted.

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

## 24 September · round 1 · the same text, stored two ways

Three runs of one unchanged script. The first is Monday's, repeated. The second
reads the same text glued into a single compressed file. The third reads eight
times as much text, stored the same way.

On your laptop:

```bash
docker start k8s                                   # the container was stopped, not deleted
cp ~/Downloads/simplewiki.txt.bz2 lab/data/
```

Then open a kubectl client exactly as round 1 of 21.09 does (`µLab 1, panel 1 ·
Start the cluster, then get a client`). Everything marked *client* below is typed
in that shell.

```bash
# client: one worker, then Monday's count, unchanged
kubectl scale deployment spark-worker --replicas=1
kubectl exec deploy/spark-master -- \
  /opt/spark/bin/spark-submit --master spark://spark-master:7077 \
  /lab/wordcount_df.py /lab/data '/lab/data/20news/*.txt'
```

The first argument is the folder holding `stopwords.txt`; the second is what gets
read.

```bash
# laptop: the same 18 846 messages, as one file. Takes about three seconds.
cd lab/data && find 20news -name '*.txt' -exec cat {} + | bzip2 > 20news.txt.bz2 && cd ../..
```

```bash
# client: the same count again, reading that file
kubectl exec deploy/spark-master -- \
  /opt/spark/bin/spark-submit --master spark://spark-master:7077 \
  /lab/wordcount_df.py /lab/data /lab/data/20news.txt.bz2

# client: and once more, on eight times the text
kubectl exec deploy/spark-master -- \
  /opt/spark/bin/spark-submit --master spark://spark-master:7077 \
  /lab/wordcount_df.py /lab/data /lab/data/simplewiki.txt.bz2
```

**Done when** three runs and six numbers are on your sheet.

**Three things to write down.** The input partition count and the clock for each
run, and whether the fifteen words changed between the first two. Then: run
`ls /lab/data` from the shell you typed the command in. It does not exist there,
and the run works without the quotes anyway. Say why the quotes matter even so.

> Observed 2026-09-23, one worker:
>
> | run | text | input partitions | elapsed |
> |---|--:|--:|--:|
> | 18 846 files | 32.7 MB | 589 | 23.0 s |
> | the same text, one `.bz2` | 32.7 MB | 2 | 4.7 s |
> | Wikipedia, one `.bz2` | 260 MB | 2 | 27.9 s |
>
> The first two returned the same fifteen words with the same counts, to the
> digit. Only the packaging differed, and it cost five times the clock.

**Extensions.** Edit the script: drop the length filter, raise `TOP_N`, add a
stopword. Then find out why 589 is neither 18 846 nor 1, with
`--conf spark.sql.files.openCostInBytes=1048576` (589 becomes 148), and move that
setting out of the command line and into `SparkSession.builder`.

---

## 24 September · round 2 · one worker, then four

One thing changes: the number of workers. Four times the cores should be four
times the speed if the work divides cleanly.

In the same kubectl client:

```bash
kubectl scale deployment spark-worker --replicas=4
kubectl get pods                                   # wait for four, all Running
# then the identical submission from round 1
```

**Done when** two clocks, their ratio, and the name of the stage that refused to
parallelise are on your sheet.

> Observed 2026-09-23 on a ten-core laptop, each worker given one core:
>
> | input | workers | input partitions | elapsed |
> |---|--:|--:|--:|
> | `.bz2` | 1 | 2 | 27.9 s |
> | `.bz2` | 4 | 4 | 14.1 s |
> | plain `.txt` | 1 | 3 | 11.6 s |
> | plain `.txt` | 4 | 4 | 8.9 s |
>
> Four times the workers bought a factor of two. Dropping the compression bought
> almost as much again, for 260 MB of disk.

**Extension.** `bzcat lab/data/simplewiki.txt.bz2 > lab/data/simplewiki.txt`, then
run at four workers and at one. Which bought more: the workers or the format?

---

## 24 September · round 3 · deploy Flyte and run a pipeline

The workflow is `hello.py`, in this repository, so the clone you already have is
all you need. Everything else is on
`week2/day2/handouts/ulab3-driver-worksheet.pdf`. It needs port 6443, which the
Spark cluster is holding, so the round opens with `docker stop k8s`.

```bash
docker stop k8s
flytectl demo start                               # answer n if it offers to delete
export FLYTECTL_CONFIG=~/.flyte/config-sandbox.yaml

cd lab
uv run --python 3.12 --with flytekit==1.16.28 \
  pyflyte run --remote hello.py count_words --sentence "the quick brown fox jumps"
```

**Done when** the console at <http://localhost:30080/console> shows one execution
of `count_words` in phase SUCCEEDED, with two tasks.

> Observed 2026-09-23: flytectl v0.9.8, Flyte sandbox v1.16.8, flytekit 1.16.28.
> A warm start took 110 s; the execution succeeded in 36.8 s. With
> `cache=True, cache_version="1.0"` on both tasks and unchanged inputs, a later
> run succeeded in **0.085 s and created no pods at all**.

---

## What is in here

| path | what |
|---|---|
| `spark-as-typed.yaml` | the three objects round 2 creates by hand, written down |
| `spark-standalone.yaml` | the same, plus the volume and the hostname; applied in round 3 |
| `data/20news/` | **not in git**: unzip `20news.zip` from ISC Learn to here |
| `data/stopwords.txt` | 203 stopwords: ordinary English, plus the mail header field names |
| `wordcount_df.py` | the count with the DataFrame API, and `explain()` |
| `wordcount_rdd.py` | the same count with RDDs, and `toDebugString()` |
| `slowdown.py` | the partition and parallelism experiment |
| `data/simplewiki.txt.bz2` | **not in git**: download from ISC Learn for 24.09 |
| `wiki_to_text.py` | turns a MediaWiki XML dump into the one-article-per-line file above |
| `hello.py` | the two-task Flyte workflow of 24.09 round 3 |

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
