# TFT Data Pipeline

<p align="center">
<img src="https://cdn.discordapp.com/attachments/690048481557217353/1203125993691160576/PngItem_4371539.png?ex=65cff565&is=65bd8065&hm=022781b6bbd31b3212342c962cdd076bc6314da1c918542aa3c4143ce04e636c&" width="210" height="130">
</p>

This personal project aims to create a pipeline to process and analyze data for the PvP strategy game [**Teamfight Tactics**](https://www.leagueoflegends.com/en-us/news/game-updates/what-is-teamfight-tactics/). The main purpose of this project is for me to learn and demonstrate key data pipeline development tools and practices. The data is procesed in an ELT pipeline, and results in a dashboard that reports unit/champion win rates, augment win rates, composition win rates, and common unit-item combinations for high ranked players on the current patch version of the game. A large portion of the game is information-based, and knowing which characters, items, augments, etc... are strong on the current patch is a crucial part of winning.

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
<img src="https://media.discordapp.net/attachments/690048481557217353/1203192380417646614/My_First_Board_-_Frame_1.jpg?ex=65d03339&is=65bdbe39&hm=e84592a95b8583d5fd5c548227508e63134bf317694441bec0e328c30379fd56&=&format=webp&width=1440&height=512" width="932" height="331">
</p>
