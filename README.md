# TFT Data Pipeline

<p align="center">
<img src="https://cdn.discordapp.com/attachments/690048481557217353/1203125993691160576/PngItem_4371539.png?ex=65e26a65&is=65cff565&hm=0dd36a880858d29909cb9a08e9f8c8ccbc896f2439bf5afcb61749ba9100bd5d&" width="210" height="130">
</p>

This personal project aims to create a pipeline to process and analyze data for the PvP strategy game [**Teamfight Tactics**](https://www.leagueoflegends.com/en-us/news/game-updates/what-is-teamfight-tactics/). The main purpose of this project is for me to learn and demonstrate key data pipeline development tools and practices. The data is procesed in an ELT pipeline, and results in a dashboard that reports unit/champion win rates, augment win rates, trait win rates, and common unit-item combinations for high ranked players on the current patch version of the game. A large portion of the game is information-based, and knowing which characters, items, augments, etc... are strong on the current patch is a crucial part of winning.

## TFT Quick Explanation: What is TFT?
TFT is essentially a PvP online board game, where 8 players face off in a series of battles (rounds) until the last person is standing. Players strategically place *champions* (characters/units) on their board with the aim of creating the strongest board in the lobby. Losing a battle depletes a player's Health Points, and the player is eliminated once this resource reaches 0. After each battle, players are given *gold*. The first way players can spend this gold is to purchase champions from a randomized selection of a shared pool (aka the player's *shop*). Players can also choose to re-roll their shop for a new selection of champions. The last way players can use their gold is to buy *exp* to level up, which allows them to place more champions on their board, and increases the odds of more expensive + stronger champions appearing in their shop. The combination of champions that a player plays is called a *composition*.

### Other Important Mechanics:
* All champions have *traits*, and placing a certain number of champions with shared traits on the board activates the traits. 
* Active traits grant power-ups in multiple tiers (depending on the number of active units with the trait) called *styles*.
* Purchasing multiple copies of the same champion levels them up (3 copies = level 2, 9 copies = level 3).
* Players can equip their champions with items, which are mainly be acquired through special rounds.
* There are 3 opportunities throughout the game for players to select *augments* that power up their entire board.

## Data
The bulk of the data is ingested from [Riot Games' developer API](https://developer.riotgames.com/). Specifically, the main data of interest is the *match summary*, which is a snapshot of the end-game state in JSON format. RiotWatcher, a light wrapper around the Riot Games API, was used for ingesting the match summary data as it automatically manages the the API request rate limits. Data that maps champion, item, trait, and augment IDs to more recognizable names is ingested from [Riot Data Dragon](https://riot-api-libraries.readthedocs.io/en/latest/ddragon.html).

**Note:** This project is not a registered product, so a Riot Games account and developer API key are required to use or reproduce this project. Also, there is currently no efficient way to get match history data without special permissions (harsh rate limits, no one-step way to request for match history data, and batch requests are disabled), thus data ingestion can be slow.

## Tools & Purpose/Usage
* [**Google Cloud Platform**](https://cloud.google.com/) - Cloud Platform
* [**Google Cloud Storage**](https://cloud.google.com/storage) - Data Lake 
* [**BigQuery**](https://cloud.google.com/bigquery) - Data Warehouse
* [**Terraform**](https://www.terraform.io/) - IaC Tool
* [**Docker**](https://www.docker.com/) & [**Docker Compose**](https://docs.docker.com/compose/) - Containerization
* [**Airflow**](https://airflow.apache.org/) - Workflow Orchestration
* [**Spark (PySpark)**](https://spark.apache.org/) - Data Processing & Transformation
* [**dbt**](https://www.getdbt.com/) - Data Transformation
* [**Looker Studio**](https://lookerstudio.google.com/) - Data Visualization

## Architecture Diagram
<p align="center">
<img src="https://cdn.discordapp.com/attachments/690048481557217353/1209590729954037770/TFT_Pipeline_-_Frame_2_1.jpg?ex=65e77a27&is=65d50527&hm=add13b24e311ae353e589836acab63cc12edce01383a8a43d39d90617db936d2&">
</p>

## Dashboard
[**Link to Dashboard**](https://lookerstudio.google.com/reporting/239efc03-642a-48f0-bef1-494fa8c0851a)

The dashboard can still be tweaked for presentation. The dashboard has currently stopped updating as of patch 14.3 due to GCP fees. If the dashboard no longer works at the time of viewing this project, please take a look at the PDF snapshot of the dashboard in the folder.

### Dashboard Purpose
In TFT, players are ranked by an algorithm with the same core concept as the *elo* system in chess. Finishing top 4 in a lobby increases their MMR (elo), while finishing bottom 4 decreases their MMR. Of course, higher finishes grant more MMR. The dashboard mainly informs players of the average placement, top 4 rate, and lobby win rate of champions, items, traits, and augments, to assist in their decision-making. The "# Times Played" field is also present on most charts to show the sample size and popularity of the selection, for the player to make a more informed decision on their confidence in the data.

## Orchestration
There are two main branches to the DAG. One for ingesting and partially transforming match history data, and the other for ingesting the data to map IDs to names (Data Dragon). They converge during the dbt task, although there is no dbt task in the DAG at the moment for the reason noted below.

* Match history branch: The current patch version of TFT is checked, and if TFT is on a new patch from the previous DAG run, the previous patch data is deleted from GCS and BigQuery. Match history data over the past day (for the current patch version) is extracted in JSON (Python library) format, parquetized, processed through PySpark on a Dataproc cluster, and loaded as tables in BigQuery (appended to the existing tables if not the first day of the patch).

* Data Dragon branch: The current id to name mappings for champions, items, traits, and augments are ingested in JSON format, parquetized, and loaded as tables in BigQuery.

Note: There is no dbt task in the DAG because it appears a Cloud Teams or Enterprise account with admin access is required to create a service token. As a solution for the time being, building the dbt models is scheduled for 1:00 am daily as a job on dbt Cloud. The main dag runs at 12:00 am every day, and should store all the tables (to be staged in dbt) in BigQuery within an hour. This may need to be changed if pulling match data for the very first time (and thus getting past 20 matches for each player), or if there is an error during the DAG run.

## Transformation
Transformations are mainly done through dbt, but the match history data is initially processed through PySpark on a Dataproc cluster, and the id to name data is initially processed locally using the pandas library. 

The Spark job extracts all of the necessary fields from match history parquet files, formats the heavily nested data into 4 separate tables, and loads them into BigQuery. The id to name data only consists of 4 small JSON files, so the necessary information is extracted, the table for items is transformed using pandas, then all of the files are parquetized and loaded as tables in BigQuery.

The dbt job performs the bulk of the transformations. After the tables are staged, some joins, selections, and groupings are performed and loaded as views in the 'intermediate' step. The final calculations + operations are then performed and loaded as tables in BigQuery.

**Notes:** 
* PySpark may not have been the most appropriate tool for the initial transformations I am performing on the match history data, but I wanted to learn basic Spark functionalities. There were initially more transformations being performed with PySpark (mainly editing strings), but mapping ids to names using data from Data Dragon meant most of these functionalities were no longer needed. 
* The dbt lineage is currently a bit messy, and the queries will be re-formatted.

## Potential Improvements and Considerations
Ideally, I would also report on the win rates of meta/popular composition archetypes. However, due to the number of archetypes and different permutations of compositions possible, analyzing good meta comps with high confidence requires manual curation or machine learning techniques such as clustering (probably using DBSCAN as the number of clusters is not determined, and can vary by TFT sets and patches), which is currently outside the scope of this project. This may be added in the future if running Dataproc jobs is not too costly, or with a smaller dataset on a new patch (maybe even done locally).
