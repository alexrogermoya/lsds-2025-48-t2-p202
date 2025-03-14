# Analyzing Tweets using Spark RDD

The goal of this lab is to use Spark RDDs to analyze a large volume of Tweets in a Spark cluster.

# Table of contents

- [Required exercises](#required-exercises)
  - [Seminar 3: Using Spark RDDs](#seminar-3-using-spark-rdds)
  - [Lab 3: Downloading Tweets and parsing them from JSON](#lab-3-downloading-tweets-from-s3-and-parsing-them-from-json)
  - [Lab 4: Analyzing Tweets with Spark](#lab-4-analyzing-tweets-with-spark)
  - [Seminar 4: Running Spark in AWS](#seminar-4-running-spark-in-aws)
- [Additional exercises](#additional-exercises)

# Required exercises

Remember you must format your code with black and follow PEP8 conventions.

> [!TIP]
> When contributing to bigger software projects professionally, following conventions and using formatters like Black allows all contributors to produce more cohesive code.

## Seminar 3: Using Spark RDDs

> Before starting this section, read [RDD Programming Guide](https://spark.apache.org/docs/latest/rdd-programming-guide.html) and [Resilient Distributed Datasets](https://www.usenix.org/system/files/conference/nsdi12/nsdi12-final138.pdf)

### [S3Q0] [10 marks] What is Spark RDD?

- **[1 mark]** What is the difference between a transformation and an action?

**A transformation is a lazy operation on an RDD that produces another RDD (e.g., map(), filter()), while an action triggers execution and returns a value (e.g., collect(), reduce()).**

- **[1 mark]** What is the difference between a wide and a narrow dependency? What is a stage in Spark RDD?

**A narrow dependency occurs when each partition of the parent RDD maps to one partition of the child RDD. A wide dependency occurs when multiple partitions of the parent RDD are shuffled to form one partition in the child RDD. A stage is a set of transformations with narrow dependencies.**

Start up a Spark cluster locally using Docker compose: `docker-compose up`.

- **[1 mark]** How many Spark workers exist in your local cluster? Take a screenshot of Docker Desktop and add it to the README.

**There exist 2 Spark workers in our local cluster. The next screenshoot shows the spark master and the 2 workers at Docker Desktop:**

![screenshoot S3Q0-1](./images/S3Q0/S3Q0-1.png)

- **[3 mark]** What is a lambda function in Python?

**A lambda function in Python is an anonymous, small function defined with the lambda keyword. It can take any number of arguments but can only have one expression. Lambda functions are often used for short, throwaway operations in functions like map(), filter(), or sorted().**

- **[3 mark]** What do the RDD operations `map`, `filter`, `groupByKey` and `flatMap` do?

**map(): Applies a function to each element of the RDD and returns a new RDD with the results.**

**filter(): Filters elements of the RDD based on a condition and returns a new RDD with the elements that meet the condition.**

**groupByKey(): Groups the elements of an RDD by their key, resulting in a new RDD where each key is paired with an iterable of values.**

**flatMap(): Similar to map(), but it can return a varying number of output elements for each input element. It flattens the results into a single RDD.**

- Check the local IP for the Spark Master service in the `spark-master-1` container logs. You should see a log similar to `Starting Spark master at spark://172.20.0.2:7077`.
- Run the job with Spark: `docker-compose exec spark-master spark-submit --master spark://{IP_FROM_PREVIOUS_STEP}:7077 /opt/bitnami/spark/app/spark_sum.py /opt/bitnami/spark/app/data/numbers1.txt`

- **[1 mark]** Take a close look at the logs. What was the result of your job?

**The result of the job is printed towards the end of the logs: SUM = 55. Here is the screenshoot:**

![screenshoot S3Q0-2](./images/S3Q0/S3Q0-2.png)

### [S3Q1] [5 marks] Sum the numbers

The file [numbers2.txt](./data/numbers2.txt) has many lines, each with many numbers.

- Create a file `spark_sum2.py`
- Implement and run a Spark job that computes the sum of all the numbers.
- Write the command you used to run it in the README and show a screenshot of the result.

**The command used to run this Spark job is `docker-compose exec spark-master spark-submit --master spark://172.20.0.2:7077 /opt/bitnami/spark/app/spark_sum2.py /opt/bitnami/spark/app/data/numbers2.txt`**

![screenshoot S3Q1](./images/S3Q1/S3Q1-1.png)

### [S3Q2] [5 marks] Sum the even numbers

The file [numbers2.txt](./data/numbers2.txt) has many lines, each with many numbers.

- Create a file `spark_sum3.py`
- Implement and run a Spark job that computes the sum of all the even numbers.
- Write the command you used to run it in the README and show a screenshot of the result.

**The command used to run this Spark job is `docker-compose exec spark-master spark-submit --master spark://172.20.0.2:7077 /opt/bitnami/spark/app/spark_sum3.py /opt/bitnami/spark/app/data/numbers2.txt`**

![screenshoot S3Q2](./images/S3Q2/S3Q2.png)

### [S3Q3] [5 marks] Find how many people live in each city

The file [people.txt](./data/people.txt) has many lines, each with `{NAME} {LANGUAGE} {CITY}`.

- Create a file `spark_count_people.py`
- Implement and run a Spark job that counts how many people live in each city.
- Write the command you used to run it in the README and show a screenshot of the result.

**The command used to run this Spark job is `docker-compose exec spark-master spark-submit --master spark://172.20.0.2:7077 /opt/bitnami/spark/app/spark_count_people.py /opt/bitnami/spark/app/data/people.txt`**

![screenshoot S3Q3](./images/S3Q3/S3Q3.png)

### [S3Q4] [5 marks] Count the bigrams

The file [cat.txt](./data/cat.txt) has many lines, each with a sentence.

- Create a file `spark_count_bigrams.py`
- Implement and run a Spark job that counts how many tines each bigram appears.
- Write the command you used to run it in the README and show a screenshot of the result.

**The command used to run this Spark job is `docker-compose exec spark-master spark-submit --master spark://172.18.0.2:7077 /opt/bitnami/spark/app/spark_count_bigrams.py /opt/bitnami/spark/app/data/cat.txt`**

![screenshoot S3Q4](./images/S3Q4/S3Q4.png)

## Lab 3: Parsing Tweets as JSON

### [L3Q0] [5 marks] The tweets dataset

- Follow the Developer Setup to download the needed data if you did not at the beginning of the course.
- Take a look at the first Tweet: `cat Eurovision3.json -n | head -n 1 | jq`. [Help](https://unix.stackexchange.com/questions/288521/with-the-linux-cat-command-how-do-i-show-only-certain-lines-by-number#:~:text=cat%20%2Fvar%2Flog%2Fsyslog%20-n%20%7C%20head%20-n%2050%20%7C,-b10%20-a10%20will%20show%20lines%2040%20thru%2060.)
- **[1 mark]** What field in the JSON object of a Tweet contains the user bio?

  The field "description" in "user"\
  ![screenshoot L3Q0-1](./images/L3Q0/L3Q0-1.png)

- **[1 mark]** What field in the JSON object of a Tweet contains the language?

  The field "lang"\
  ![screenshoot L3Q0-2](./images/L3Q0/L3Q0-2.png)

- **[1 mark]** What field in the JSON object of a Tweet contains the text content?

  The field "text"\
  ![screenshoot L3Q0-3](./images/L3Q0/L3Q0-3.png)

- **[1 mark]** What field in the JSON object of a Tweet contains the number of followers?

  The field "followers_count" in "user"\
  ![screenshoot L3Q0-4](./images/L3Q0/L3Q0-4.png)

- Take a look at the first two lines: `cat Eurovision3.json -n | head -n 2`.
- **[1 mark]** How many Tweets does each line contain?\
  Each line contains one Tweet\
  ![screenshoot L3Q0-5](./images/L3Q0/L3Q0-5.png)\
  ![screenshoot L3Q0-6](./images/L3Q0/L3Q0-6.png)

### [L3Q1] [5 marks] Parsing JSON with Python

- Create a file `tweet_parser.py`
- Create a `Tweet` dataclass with fields for the `tweet_id` (int), `text` (str), `user_id` (int), `user_name` (str), `language` (str), `timestamp_ms` (int) and `retweet_count` (int). [Help](https://realpython.com/python-data-classes/)
- Create a function `parse_tweet(tweet: str) -> Tweet` that takes in a Tweet as a Json string and returns a Tweet object. [Help](https://stackoverflow.com/a/7771071)
- Read the first line of `Eurovision3.json` and print the result of `parse_tweet`. [Help](https://stackoverflow.com/questions/1904394/read-only-the-first-line-of-a-file)
- Take a screenshot and add it to the README.

![screenshot L3Q1](./images/L3Q1/L3Q1.png)

### [L3Q2] [5 marks] Counting Tweets by language

- Create a file `simple_tweet_language_counter.py`
- Implement a script that reads each line of `Eurovision3.json` one by one. [Help](https://stackoverflow.com/a/3277512)
  - You might need to skip any invalid lines, such as empty lines with only a `\n` or Tweets with an invalid JSON format.
- Parse each Tweet using the `parse_tweet` function from the previous exercise.
- Count the number of Tweets of each language using a dictionary. [Help](https://www.w3schools.com/python/python_dictionaries.asp)
- Print the dictionary. Take a screenshot and add it to the README.

![screenshot L3Q2](./images/L3Q2/L3Q2.png)

## Lab 4: Analyzing Tweets with Spark

### [L4Q0] [10 marks] Filtering Tweets by language with Spark

- Create a file `spark_tweet_language_filter.py`.
- Implement a Spark job that finds all the tweets in a file for a given language (e.g. `zh`)
- Saves the result to a file
- Run your code in your local Spark cluster:

```zsh
docker-compose exec spark-master spark-submit --master spark://{IP_FROM_PREVIOUS_STEP}:7077 /opt/bitnami/spark/app/spark_tweet_language_filter.py zh /opt/bitnami/spark/app/data/Eurovision3.json /opt/bitnami/spark/output/Eurovision3Zh.json
```

> You might need to `chmod 755 data` if you get "file not found" errors

### [L4Q1] [10 marks] Get the most repeated bigrams

- Create a file `spark_tweet_bigrams.py`.
- Implement a Spark job that finds the most repeated bigrams for a language (e.g. `es`)
- Filter out bigrams that only appear once
- Saves the result to a file (sorted by how many times they appear in descending order)
- Run your code in your local Spark cluster:

```zsh
docker-compose exec spark-master spark-submit --master spark://{IP_FROM_PREVIOUS_STEP}:7077 /opt/bitnami/spark/app/spark_tweet_bigrams.py es /opt/bitnami/spark/app/data/Eurovision3.json /opt/bitnami/spark/output/Eurovision3EsBigrams
```

### [L4Q2] [10 marks] Get the 10 most retweeted tweets

- Create a file `spark_tweet_retweets.py`.
- Implement a Spark job that finds the users with the top 10 most retweeted Tweets for a language
- Run your code in your local Spark cluster:

```zsh
docker-compose exec spark-master spark-submit --master spark://{IP_FROM_PREVIOUS_STEP}:7077 /opt/bitnami/spark/app/spark_tweet_retweets.py es /opt/bitnami/spark/app/data/Eurovision3.json
```

### [L4Q3] [10 marks] Get the 10 most retweeted users

- Create a file `spark_tweet_user_retweets.py`.
- Implement a Spark job that finds the users with the top 10 most retweets (in total) for a language and how many retweets they have. I.e., sum all the retweets each user has and get the top 10 users.
- Run your code in your local Spark cluster:

```zsh
docker-compose exec spark-master spark-submit --master spark://{IP_FROM_PREVIOUS_STEP}:7077 /opt/bitnami/spark/app/spark_tweet_user_retweets.py es /opt/bitnami/spark/app/data/Eurovision3.json
```

## Seminar 4: Running Spark in AWS

AWS allows us to rent virtual servers and deploy a Spark cluster to do data anlysis at scale. In this seminar, you will learn how to:

- Use S3 to store and read files
- Use AWS EMR to host a Spark cluster in AWS EC2 servers
- Run some of your Spark applications in the cluster.

### [S4Q0] [10 marks] Run L4Q1 in AWS using EMR

- Accept the invitation to AWS academy.
- Open the [AWS Academy](https://awsacademy.instructure.com/courses) course
- In `Modules`, select `Launch AWS Academy Learner Lab`
- Click `Start Lab`
- Wait until the `AWS` indicator has a green circle
- Click the `AWS` text with the green circle to open the AWS console

> [!TIP]
> When you launch a cluster, you start spending AWS credit! Remember to terminate your cluster at the end of your experiments!

- [Create a bucket in S3](https://us-east-1.console.aws.amazon.com/s3/home?region=us-east-1#):

  - Bucket type: `General purpose`
  - Name: `lsds-2025-{group_number}-t{theory_number}-p{lab_number}-s{seminar_number}-s3bucket`

- Paste a screenshot

![screenshot S4Q0-1](./images/S4Q0/S4Q0-1.png)

- In the bucket, create 4 folders: `input`, `app`, `logs` and `output`

- Paste a screenshot

![screenshot S4Q0-2](./images/S4Q0/S4Q0-2.png)

- Upload the `Eurovision3.json` file inside the `input` folder

- Paste a screenshot

![screenshot S4Q0-3](./images/S4Q0/S4Q0-3.png)

- Upload `spark_tweet_user_retweets.py` and `tweet_parser.py` in the `app` folder

- Paste a screenshot

![screenshot S4Q0-4](./images/S4Q0/S4Q0-4.png)

- Open the [EMR console](https://us-east-1.console.aws.amazon.com/emr/home?region=us-east-1#/clusters)

- Create a cluster
  - Application bundle: `Spark Interactive`
  - Name: `lsds-2025-{group_number}-t{theory_number}-p{lab_number}-s{seminar_number}-sparkcluster`
  - Choose this instance type: `m4.large`
  - Instance(s) size: `3`
  - Cluster logs: select the `logs` folder in the S3 bucket you created
  - Service role: `EMR_DefaultRole`
  - Instance profile: `EMR_EC2_DefaultRole`
- Paste a screenshot

![screenshot S4Q0-5](./images/S4Q0/S4Q0-5.png)

- In `Steps`, select `Add step`.

  - Type: `Spark application`
  - Name: `lab2-ex13`
  - Deploy mode: `Cluster mode`
  - Application location: select the `spark_tweet_user_retweets.py` in the S3 bucket
  - Spark-submit options: specify the `tweet_parser.py` module. For example: `--py-files s3://lsds-2025-miquel-test/app/tweet_parser.py`
  - Arguments: specify the input and output. For example: `es s3://lsds-2025-miquel-test/input/Eurovision3.json`.

- Paste a screenshot

![screenshot S4Q0-6](./images/S4Q0/S4Q0-6.png)

- When you submit a step, wait until the `Status` is `Completed`.

- Paste a screenshot

![screenshot S4Q0-7](./images/S4Q0/S4Q0-7.png)

> [!TIP]
> You can find the logs in your S3 bucket: `logs/{cluster id}/containers/application_*_{run number}/container_*_000001/stdout.gz` - they might take some minutes to appear

- Paste a screenshot of the log where we can see: how much time it took, what are the ids of the ten most retweeted users.

![screenshot S4Q0-8](./images/S4Q0/S4Q0-8.png)

*Note: this is not the correct output, since we did this previously to change the code to have less RT counts. After changes, we have next:*

Top 10 most retweeted users in es:
1. User: carmen_abcdd, Total retweets: 5834
2. User: NetflixES, Total retweets: 5502
3. User: LVPibai, Total retweets: 4932
4. User: LVPibai, Total retweets: 3933
5. User: masorhu, Total retweets: 3736
6. User: serperort, Total retweets: 3492
7. User: carloscarmo98, Total retweets: 2945
8. User: eurovision_tve, Total retweets: 2102
9. User: ManuGuix, Total retweets: 1997
10. User: _Parao, Total retweets: 1690

# Additional exercises

You can earn an additional 2 marks (over 10) on this project's grade by working on additional exercises. To earn the full +2, you need to complete 4 additional exercises.

During these exercises, you will build a (super simple) search engine, like a barebones Google.

### [AD1Q0] Crawling

Find the latest available Wikipedia datasets from [dumps.wikimedia](https://dumps.wikimedia.org/other/enterprise_html/runs/). For example, `https://dumps.wikimedia.org/other/enterprise_html/runs/20240901/enwiki-NS0-20240901-ENTERPRISE-HTML.json.tar.gz`.

Then, download the first 10, 100, 1k, 10k and 100k articles in different files. The smaller datasets will be useful for testing (replace `$1` with how many articles you want to download).

```zsh
curl -L https://dumps.wikimedia.org/other/enterprise_html/runs/20240901/enwiki-NS0-20240901-ENTERPRISE-HTML.json.tar.gz | tar xz --to-stdout | head -n $1 > wikipedia$1.json
```

Paste the first Wikipedia article here, properly formatted as JSON.

**Command used:** 

*for N in 10 100 1000 10000 100000; do     curl -L https://dumps.wikimedia.org/other/enterprise_html/runs/20250220/enwiki-NS0-20250220-ENTERPRISE-HTML.json.tar.gz | tar xz --to-stdout | head -n $N > wikipedia_$N.json; done*

**First Wikipedia article:**

*{"name":"Amantia (insect)","identifier":76712455,"date_created":"2024-04-23T08:28:58Z","date_modified":"2024-04-23T08:37:57Z","date_previously_modified":"2024-04-23T08:36:03Z","version":{"identifier":1220358148,"comment":"Added tags to the page using [[Wikipedia:Page Curation|Page Curation]] (unreferenced, context)","tags":["pagetriage"],"scores":{"revertrisk":{"probability":{"false":0.8242294788360596,"true":0.17577052116394043}}},"editor":{"identifier":24013162,"name":"CycloneYoris","edit_count":63394,"groups":["extendedconfirmed","extendedmover","patroller","reviewer","rollbacker","*","user","autoconfirmed"],"is_patroller":true,"date_started":"2015-02-05T01:34:31Z"},"number_of_characters":184,"size":{"value":184,"unit_text":"B"},"maintenance_tags":{}},"previous_version":{"identifier":1220358008,"number_of_characters":101},"url":"https://en.wikipedia.org/wiki/Amantia_(insect)","namespace":{"identifier":0},"in_language":{"identifier":"en"},"categories":[{"name":"Category:Articles with 'species' microformats","url":"https://en.wikipedia.org/wiki/Category:Articles_with_'species'_microformats"},{"name":"Category:Taxobox articles possibly missing a taxonbar","url":"https://en.wikipedia.org/wiki/Category:Taxobox_articles_possibly_missing_a_taxonbar"},{"name":"Category:Articles with short description","url":"https://en.wikipedia.org/wiki/Category:Articles_with_short_description"},{"name":"Category:Short description with empty Wikidata description","url":"https://en.wikipedia.org/wiki/Category:Short_description_with_empty_Wikidata_description"}],"templates":[{"name":"Template:Short description","url":"https://en.wikipedia.org/wiki/Template:Short_description"},{"name":"Template:Automatic taxobox","url":"https://en.wikipedia.org/wiki/Template:Automatic_taxobox"}],"is_part_of":{"identifier":"enwiki","url":"https://en.wikipedia.org"},"article_body":{"html":"\u003c!DOCTYPE html\u003e\n\u003chtml prefix=\"dc: http://purl.org/dc/terms/ mw: http://mediawiki.org/rdf/\" about=\"https://en.wikipedia.org/wiki/Special:Redirect/revision/1220358008\"\u003e\u003chead prefix=\"mwr: https://en.wikipedia.org/wiki/Special:Redirect/\"\u003e\u003cmeta charset=\"utf-8\"/\u003e\u003cmeta property=\"mw:pageId\" content=\"76712455\"/\u003e\u003cmeta property=\"mw:pageNamespace\" content=\"0\"/\u003e\u003clink rel=\"dc:replaces\" resource=\"mwr:revision/1220357490\"/\u003e\u003cmeta property=\"mw:revisionSHA1\" content=\"f564c47e364b5126ba0d15c501c421f570f121af\"/\u003e\u003cmeta property=\"dc:modified\" content=\"2024-04-23T08:36:03.000Z\"/\u003e\u003cmeta property=\"mw:htmlVersion\" content=\"2.8.0\"/\u003e\u003cmeta property=\"mw:html:version\" content=\"2.8.0\"/\u003e\u003clink rel=\"dc:isVersionOf\" href=\"//en.wikipedia.org/wiki/Amantia_(insect)\"/\u003e\u003cbase href=\"//en.wikipedia.org/wiki/\"/\u003e\u003ctitle\u003e\u0026lt;i\u003eAmantia\u0026lt;/i\u003e (insect)\u003c/title\u003e\u003clink rel=\"stylesheet\" href=\"/w/load.php?lang=en\u0026amp;modules=mediawiki.skinning.content.parsoid%7Cmediawiki.skinning.interface%7Csite.styles\u0026amp;only=styles\u0026amp;skin=vector\"/\u003e\u003cmeta http-equiv=\"content-language\" content=\"en\"/\u003e\u003cmeta http-equiv=\"vary\" content=\"Accept\"/\u003e\u003c/head\u003e\u003cbody id=\"mwAA\" lang=\"en\" class=\"mw-content-ltr sitedir-ltr ltr mw-body-content parsoid-body mediawiki mw-parser-output\" dir=\"ltr\"\u003e\u003csection data-mw-section-id=\"0\" id=\"mwAQ\"\u003e\u003cdiv class=\"shortdescription nomobile noexcerpt noprint searchaux\" style=\"display:none\" about=\"#mwt1\" typeof=\"mw:Transclusion\" data-mw='{\"parts\":[{\"template\":{\"target\":{\"wt\":\"Short description\",\"href\":\"./Template:Short_description\"},\"params\":{\"1\":{\"wt\":\"Genus of lanternfly\"}},\"i\":0}}]}' id=\"mwAg\"\u003eGenus of lanternfly\u003c/div\u003e\u003clink rel=\"mw:PageProp/Category\" href=\"./Category:Articles_with_short_description\" about=\"#mwt1\"/\u003e\u003clink rel=\"mw:PageProp/Category\" href=\"./Category:Short_description_with_empty_Wikidata_description\" about=\"#mwt1\" id=\"mwAw\"/\u003e\n\u003cp about=\"#mwt2\" typeof=\"mw:Transclusion\" data-mw='{\"parts\":[{\"template\":{\"target\":{\"wt\":\"Automatic taxobox\\n\",\"href\":\"./Template:Automatic_taxobox\"},\"params\":{\"taxon\":{\"wt\":\"Amantia\"},\"authority\":{\"wt\":\"Stal, 1864\"}},\"i\":0}}]}' id=\"mwBA\"\u003e\u003cstyle data-mw-deduplicate=\"TemplateStyles:r1219595023\" typeof=\"mw:Extension/templatestyles\" about=\"#mwt5\" data-mw='{\"name\":\"templatestyles\",\"attrs\":{\"src\":\"Template:Taxobox/core/styles.css\"}}'\u003ehtml.skin-theme-clientpref-night .mw-parser-output .infobox.biota tr{background:transparent!important}html.skin-theme-clientpref-night .mw-parser-output .infobox.biota img{background:white}@media(prefers-color-scheme:dark){html.skin-theme-clientpref-os .mw-parser-output .infobox.biota tr{background:transparent!important}html.skin-theme-clientpref-os .mw-parser-output .infobox.biota img{background:white}}\u003c/style\u003e\u003c/p\u003e\u003cspan about=\"#mwt2\"\u003e\n\u003c/span\u003e\u003ctable class=\"infobox biota\" style=\"text-align: left; width: 200px; font-size: 100%\" about=\"#mwt2\"\u003e\n\u003ctbody\u003e\u003ctr\u003e\n\u003cth colspan=\"2\" style=\"text-align: center; background-color: rgb(235,235,210)\"\u003e\u003ci\u003eAmantia\u003c/i\u003e\u003c/th\u003e\u003c/tr\u003e\n\n\n\n\u003ctr style=\"text-align: center; background-color: rgb(235,235,210)\"\u003e\u003c/tr\u003e\n\n\n\u003ctr\u003e\n\u003cth colspan=\"2\" style=\"min-width:15em; text-align: center; background-color: rgb(235,235,210)\"\u003e\u003ca rel=\"mw:WikiLink\" href=\"./Taxonomy_(biology)\" title=\"Taxonomy (biology)\"\u003eScientific classification\u003c/a\u003e \u003cspan class=\"plainlinks\" style=\"font-size:smaller; float:right; padding-right:0.4em; margin-left:-3em;\"\u003e\u003cspan typeof=\"mw:File\" data-mw='{\"caption\":\"Edit this classification\"}'\u003e\u003ca href=\"./Template:Taxonomy/Amantia\" title=\"Edit this classification\"\u003e\u003cimg alt=\"Edit this classification\" resource=\"./File:OOjs_UI_icon_edit-ltr.svg\" src=\"//upload.wikimedia.org/wikipedia/commons/thumb/8/8a/OOjs_UI_icon_edit-ltr.svg/15px-OOjs_UI_icon_edit-ltr.svg.png\" decoding=\"async\" data-file-width=\"20\" data-file-height=\"20\" data-file-type=\"drawing\" height=\"15\" width=\"15\" srcset=\"//upload.wikimedia.org/wikipedia/commons/thumb/8/8a/OOjs_UI_icon_edit-ltr.svg/23px-OOjs_UI_icon_edit-ltr.svg.png 1.5x, //upload.wikimedia.org/wikipedia/commons/thumb/8/8a/OOjs_UI_icon_edit-ltr.svg/30px-OOjs_UI_icon_edit-ltr.svg.png 2x\" class=\"mw-file-element\"/\u003e\u003c/a\u003e\u003c/span\u003e\u003c/span\u003e\u003c/th\u003e\u003c/tr\u003e\n\u003ctr\u003e\n\u003ctd\u003eDomain:\u003c/td\u003e\n\u003ctd\u003e\u003ca rel=\"mw:WikiLink\" href=\"./Eukaryote\" title=\"Eukaryote\"\u003eEukaryota\u003c/a\u003e\u003c/td\u003e\u003c/tr\u003e\n\u003ctr\u003e\n\u003ctd\u003eKingdom:\u003c/td\u003e\n\u003ctd\u003e\u003ca rel=\"mw:WikiLink\" href=\"./Animal\" title=\"Animal\"\u003eAnimalia\u003c/a\u003e\u003c/td\u003e\u003c/tr\u003e\n\u003ctr\u003e\n\u003ctd\u003ePhylum:\u003c/td\u003e\n\u003ctd\u003e\u003ca rel=\"mw:WikiLink\" href=\"./Arthropod\" title=\"Arthropod\"\u003eArthropoda\u003c/a\u003e\u003c/td\u003e\u003c/tr\u003e\n\u003ctr\u003e\n\u003ctd\u003eClass:\u003c/td\u003e\n\u003ctd\u003e\u003ca rel=\"mw:WikiLink\" href=\"./Insect\" title=\"Insect\"\u003eInsecta\u003c/a\u003e\u003c/td\u003e\u003c/tr\u003e\n\u003ctr\u003e\n\u003ctd\u003eOrder:\u003c/td\u003e\n\u003ctd\u003e\u003ca rel=\"mw:WikiLink\" href=\"./Hemiptera\" title=\"Hemiptera\"\u003eHemiptera\u003c/a\u003e\u003c/td\u003e\u003c/tr\u003e\n\u003ctr\u003e\n\u003ctd\u003eSuborder:\u003c/td\u003e\n\u003ctd\u003e\u003ca rel=\"mw:WikiLink\" href=\"./Auchenorrhyncha\" title=\"Auchenorrhyncha\"\u003eAuchenorrhyncha\u003c/a\u003e\u003c/td\u003e\u003c/tr\u003e\n\u003ctr\u003e\n\u003ctd\u003eInfraorder:\u003c/td\u003e\n\u003ctd\u003e\u003ca rel=\"mw:WikiLink\" href=\"./Planthopper\" title=\"Planthopper\"\u003eFulgoromorpha\u003c/a\u003e\u003c/td\u003e\u003c/tr\u003e\n\u003ctr\u003e\n\u003ctd\u003eFamily:\u003c/td\u003e\n\u003ctd\u003e\u003ca rel=\"mw:WikiLink\" href=\"./Fulgoridae\" title=\"Fulgoridae\"\u003eFulgoridae\u003c/a\u003e\u003c/td\u003e\u003c/tr\u003e\n\u003ctr\u003e\n\u003ctd\u003eTribe:\u003c/td\u003e\n\u003ctd\u003e\u003ca rel=\"mw:WikiLink\" href=\"./Poiocerini\" title=\"Poiocerini\" class=\"mw-redirect\"\u003ePoiocerini\u003c/a\u003e\u003c/td\u003e\u003c/tr\u003e\n\u003ctr\u003e\n\u003ctd\u003eGenus:\u003c/td\u003e\n\u003ctd\u003e\u003ca rel=\"mw:WikiLink\" href=\"./Amantia_(planthopper)\" title=\"Amantia (planthopper)\"\u003e\u003ci\u003eAmantia\u003c/i\u003e\u003c/a\u003e\u003cbr/\u003e\u003csmall\u003eStal, 1864\u003c/small\u003e\u003c/td\u003e\u003c/tr\u003e\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\u003ctr style=\"text-align: center; background-color: rgb(235,235,210)\"\u003e\u003c/tr\u003e\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\u003c/tbody\u003e\u003c/table\u003e\u003clink rel=\"mw:PageProp/Category\" href=\"./Category:Articles_with_'species'_microformats\" about=\"#mwt2\"/\u003e\u003clink rel=\"mw:PageProp/Category\" href=\"./Category:Taxobox_articles_possibly_missing_a_taxonbar\" about=\"#mwt2\" id=\"mwBQ\"/\u003e\u003c/section\u003e\u003c/body\u003e\u003c/html\u003e","wikitext":"{{Multiple issues|\n{{unreferenced|date=April 2024}}\n{{context|date=April 2024}}\n}}\n{{Short description|Genus of lanternfly}}\n{{Automatic taxobox\n|taxon=Amantia\n|authority=Stal, 1864\n}}"},"license":[{"name":"Creative Commons Attribution Share Alike 3.0 Unported","identifier":"CC-BY-SA-3.0","url":"https://creativecommons.org/licenses/by-sa/3.0/"}],"event":{"identifier":"2875e9a6-05ec-4bfe-975b-a6ed058779cf","type":"update","date_created":"2024-04-23T08:37:59.279464Z","date_published":"2024-04-23T08:37:59.61309Z"}}*

**NOTE: We do not upload the different wikipedia_N.json files, since the larger ones are too heavy. We attach how the different files have been created:**

![screenshot AD1Q0](./images/AD1/AD1Q0.png)

### [AD1Q1] Building the repository

Write a Python or bash script that splits the big file into multiple files, one file per line. The file name should be the identifier of the article, and the content of the file the full JSON object.

**Next is the Python script required:**  [split_wikipedia.py](./additional/split_wikipedia.py)

Run said script for the 10 and 1k datasets.

**The following screenshot shows how it worked correctly for both examples:**

![screenshot AD1Q1-1](./images/AD1/AD1Q1-1.png)

**The following screenshot is a concrete example of the start of a file:**

![screenshot AD1Q1-2](./images/AD1/AD1Q1-2.png)

### [AD1Q2] Building the reverse index

Write a Spark RDD job that creates a reverse index for all the crawled articles.

The reverse index must map every word in the abstract of every article, to the list of article (ids) that contain it. Store this as a file. The format must be: `LINE CRLF LINE CRLF LINE CRLF ...`, where each `LINE` is `WORD SP DOCID SP DOCID SP DOCID SP ... DOCID`. For example:

```
seven 18847712 76669474 76713187 75388615 1882504 18733291 19220717 3118126 31421710 26323888 52867888 76712306 76711442 48957757
seasons 58765506 76669474 7755966 66730851 53056676 40360169 7871468 60331788 52867888 70406270 52243132 17781886
22 12000256 14177667 56360708 50648266 31581711 76395922 31418962 73082202 33375130 76669474 76713187 5799657 40360169 65704112 18688178 48850419 37078259 63141238 40538167 32644089
due 76731844 41098246 25214406 41098253 1658830 31581711 8905616 45711377 14259409 76708884 2723548 76732829 1122974 41233503 43331165 76669474 12365159 18733291 7871468 65704112 63447415 63840761 68538426 36367677
sold 76669474 31728882 53538197 63141238 12243595
```

Remember to strip all symbols and make the text lowercase before indexing. For example: `hello`, `HELLO`, `HeLLo`, `hello,`, `hello?`, `[hello]` and `hello!` must all be treated as the same word.

Customize the partitioner function to be `ord(key[0]) % PARTITION_COUNT`, such that we can easily know in which partition a word will be in the inverse index. Make `PARTITION_COUNT` a parameter. Verify that the words are indeed in the correct partition.

Test it locally with the 10 and 100 datasets.

### [AD1Q3] Build a search API

Create a FastAPI service that, for a given query of space-separated words, returns the name, abstract, identifier and URL of all Wikipedia articles that contain all those words.

The API should look like this:

```
curl -X POST localhost:8080/search -H "Content-Type: application/json" -d '{
    "query": "english football season"
}' | jq

{
  "results": [
    {
      "name": "1951–52 Southern Football League",
      "abstract": "The 1951–52 Southern Football League season was the 49th in the history of the league, an English football competition. At the end of the previous season Torquay United resigned their second team from the league. No new clubs had joined the league for this season so the league consisted of 22 remaining clubs. Merthyr Tydfil were champions for the third season in a row, winning their fourth Southern League title. Five Southern League clubs applied to join the Football League at the end of the season, but none were successful.",
      "identifier": 32644089,
      "url": "https://en.wikipedia.org/wiki/1951%E2%80%9352_Southern_Football_League"
    },
    {
      "name": "1997–98 Blackburn Rovers F.C. season",
      "abstract": "During the 1997–98 English football season, Blackburn Rovers competed in the FA Premier League.",
      "identifier": 29000478,
      "url": "https://en.wikipedia.org/wiki/1997%E2%80%9398_Blackburn_Rovers_F.C._season"
    },
    {
      "name": "1993 Football League Cup final",
      "abstract": "The 1993 Football League Cup final took place on 18 April 1993 at Wembley Stadium, and was played between Arsenal and Sheffield Wednesday. Arsenal won 2–1 in normal time, in what was the first of three Wembley finals between the two sides that season; Arsenal and Wednesday also met in the FA Cup final of that year, the first time ever in English football. The match was the first match in which any European clubs had used squad numbers and player names on their shirts. On this occasion, as in the FA Cup final and replay that year, players wore individual numbers which were retained for the FA Cup finals. Coincidentally, the first occurrence of players wearing numbered shirts came on 25 August 1928, when Arsenal and Chelsea wore numbered shirts in their matches against The Wednesday and Swansea Town, respectively. Squad numbers became compulsory for Premier League clubs from August 1993. In the game, Wednesday's John Harkes scored the opener in the 8th minute, before Paul Merson equalised for Arsenal. Merson then set up Steve Morrow for the winner. In the celebrations after the match, Arsenal skipper Tony Adams attempted to pick up Morrow and parade him on his shoulders, but Adams slipped and Morrow awkwardly hit the ground. He broke his arm and had to be rushed to hospital. Unable to receive his winner's medal on the day, he was eventually presented with it before the start of the FA Cup Final the following month.",
      "identifier": 7902567,
      "url": "https://en.wikipedia.org/wiki/1993_Football_League_Cup_final"
    },
    {
      "name": "1989–90 Middlesbrough F.C. season",
      "abstract": "During the 1989–90 English football season, Middlesbrough F.C. competed in the Football League Second Division.",
      "identifier": 59075107,
      "url": "https://en.wikipedia.org/wiki/1989%E2%80%9390_Middlesbrough_F.C._season"
    }
  ]
}
```

Some tips:

- Read the inverse index you created with Spark from the file system to know which documents contain any given word.
- Use set intersections to find the document ids that contain all the query words.
- Read the files from the file system repository you created in AD1Q1 to find the abstract, uri and title for any given id.
