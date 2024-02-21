# TFT Data Pipeline

<p align="center">
<img src="https://cdn.discordapp.com/attachments/690048481557217353/1203125993691160576/PngItem_4371539.png?ex=65e26a65&is=65cff565&hm=0dd36a880858d29909cb9a08e9f8c8ccbc896f2439bf5afcb61749ba9100bd5d&" width="210" height="130">
</p>

This personal project aims to create a pipeline to process and analyze data for the PvP strategy game [**Teamfight Tactics**](https://www.leagueoflegends.com/en-us/news/game-updates/what-is-teamfight-tactics/). The main purpose of this project is for me to learn and demonstrate key data pipeline development tools and practices. The data is procesed in an ELT pipeline, and results in a dashboard that reports unit/champion win rates, augment win rates, trait win rates, and common unit-item combinations for high ranked players on the current patch version of the game. A large portion of the game is information-based, and knowing which characters, items, augments, etc... are strong on the current patch is a crucial part of winning.

## TFT Quick Explanation
TFT is essentially a PvP online board game, where 8 players face off in a series of battles (rounds) until the last person is standing. Players purchase *champions* (characters/units) from a randomized selection of a shared pool, and strategically place them on their board between each battle. All champions have *traits*, and combining a certain number of units with shared traits grants power-ups. The combination of champions that a player plays is called a *composition*. Purchasing multiple copies of the same champion levels them up (max level 3), and players can also equip their champions with items. Finally, there are 3 opportunities throughout the game for players to choose *augments* that power up their entire board.

## Data
The data is ingested from [Riot Games' developer API](https://developer.riotgames.com/). Specifically, the main data of interest is the *match summary*, which is a summary of the end-game state in JSON format. RiotWatcher, a light wrapper around the Riot Games API, was used for ingestion as it automatically manages the the API request rate limits.

**Note:** There is currently no efficient way to get match history data without special permissions (harsh rate limits, no one-step way to request for match history data, and batch requests are disabled), thus data ingestion can be slow. This project is not a registered product, so a Riot Games account and developer API key are required to use or reproduce this project.

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
The dashboard is still in progress, please see it in its current state from the link above. The dashboard will eventually stop updating as the DAG runs begin to incur costs on GCP.
Ideally, I would also report on the win rates of meta/popular composition archetypes. However, due to the different permutations of compositions possible, analyzing good meta comps with high confidence ideally requires machine learning techniques such as cluster analysis, which is outside the scope of this project.

