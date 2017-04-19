# Auto-complete

## Usage

```shell
- hadoop jar Ngram.jar com.wttttt.hadoop.Driver inputDir outputDirForJob1 outputDirForJob2 NoGram threshold topK 
```



## Introduction

* Application situation：
  * google suggestion：Between words
  * spelling correction：In one word, between characters
* N-Gram: n connective items
* Language Model：Using probability distribution to solve the problem.
* Raw Data(Input): Any semantic input
* Output: input phrase —> asscociation phrase —> probability.    [By MapReduce Job, Offline]




## MapReduce Design

### Job1: SplitNgram

* Map01: 

  * Function: get all connective items, based on given N.

  * input: sentence(not line)

  * output: key —> word1 word2 … wordi     (2 <= i <= N)

    ​		value —> 1

* Combiner01: same with Reducer01

* Reduce01: 
  * Function: count
  * Output: key —> w1 w2 … wi, value —> count(w1 w2 … wi)

### Job2: CountNgram

* Map02:
  * Function: realizing N to 1 split, and filter the records whose count < threshold.
  * Input: key —>  w1 w2 … wi, value —> count(w1 w2 … wi)
  * Output:  key —> w1 w2 … wi-1,  value —> wi=count
* Combiner02: with same function of reduce02. But of different output type.
* Reduce02:
  * function: Select topK for each key, by using priorityQueue.
  * Output: DBOutputWritable(DIY), NullWritable

