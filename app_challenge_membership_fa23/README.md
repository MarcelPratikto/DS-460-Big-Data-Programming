# Github Repo

[MarcelPratikto/app_challenge_membership_fa23](https://github.com/MarcelPratikto/app_challenge_membership_fa23)

# How to Run

### Docker Information

Within this repository, you can simply run `docker compose up` to leverage the `docker-compose.yaml` with your local folder synced with the container folder where the streamlit app is running. 

Additionally, you can use `docker build -t streamlit .` to use the `Dockerfile` to build the image and then use `docker run -p 8501:8501 -v "$(pwd):/app:rw" streamlit` to start the container with the appropriate port and volume settings.

We currently use `python:3.11.6-slim` in our [Dockerfile](Dockerfile).  You can change to `FROM quay.io/jupyter/minimal-notebook` to use the [minimal Jupyter notebook](https://quay.io/organization/jupyter)

# Feature Challenge

Spatial Map is included on my app

Code is included in [feature_scripts/marcel_pratikto_features](feature_scripts/marcel_pratikto_features.html)

# Code Evaluation Challenge

## Cmd 5
<details>
<summary>What the code is doing (CLICK TO EXPAND)</summary>

The code starts by creating a few dataframes that combines spatial data with poi data, filter down to the places in Idaho. It also filters down the pattern and tract_table dataframes to Idaho. The result from the filters are stored into their own dataframe: poi_idaho, spatial_idaho, pattern_idaho, and tract_table.


The end of the code starts by creating a new sql database called chapel. All the dataframes mentioned above, in addition to a few more dataframes created in the Cmd before Cmd 5, will be stored as tables in the chapel database.

</details>
<details>
<summary>Bugs found</summary>
N/A
</details>
<details>
<summary>Code improvements</summary>
Since the spatial dataframe is joined with poi, based on the placekey in the spatial dataframe, then filtered to just Idaho, I'm not sure if there's a need to create poi_idaho.
</details>

## Cmd 22
<details>
<summary>What the code is doing</summary>
This code is gathering all the chapels where Sunday is the most popular day, followed by Monday for all places in the US. It takes data from chapel.pattern and saves the table at chapel.use_pattern_chapel.
</details>
<details>
<summary>Bugs found</summary>
N/A
</details>
<details>
<summary>Code improvements</summary>
N/A
</details>

## Cmd 50
<details>
<summary>What the code is doing</summary>
Takes the chapel.chapel_nearest table and combines it with chapel.use_pattern_chapel. It makes sure that the address of the data from safegraph aligns with the data from the church's web scrape within 0.0003 of some measure of distance in the same city. Then it filters down the data again to chapels in which Sunday is the most popular day.
</details>
<details>
<summary>Bugs found</summary>
There are some values where Sunday ranking is null, but it's still included in the code
</details>
<details>
<summary>Code improvements</summary>
N/A
</details>

## Cmd 60
<details>
<summary>What the code is doing</summary>
Creates min, median, and max of the estimated number of visitors on Sunday for each chapels. It calculates this by taking the data from the span of a year.
</details>
<details>
<summary>Bugs found</summary>
N/A
</details>
<details>
<summary>Code improvements</summary>
N/A
</details>

## Cmd 64
<details>
<summary>What the code is doing</summary>
Removes unique visits from the estimated Sunday visitors. If that person visited a chapel that is further than two standard deviations from the average distance to the nearest chapel, then that visit isn't counted as part of the regular Sunday attendance for that county.
</details>
<details>
<summary>Bugs found</summary>
N/A
</details>
<details>
<summary>Code improvements</summary>
N/A
</details>

## Cmd 66
<details>
<summary>What the code is doing</summary>
Expands upon the active members estimate from a county level to a tract level.
</details>
<details>
<summary>Bugs found</summary>
N/A
</details>
<details>
<summary>Code improvements</summary>
N/A
</details>

## Cmd 74
<details>
<summary>What the code is doing</summary>
Creates the final table that stores the population, number of LDS members, ratio of LDS members, and ratio of religious members. It does a simple division to get the ratio of LDS members and ratio of the population that are LDS. It stores this table in membership.county_active_population_ldscensus.
</details>
<details>
<summary>Bugs found</summary>
N/A
</details>
<details>
<summary>Code improvements</summary>
N/A
</details>

# Vocabulary/Lingo Challenge

1. Explain the added value of using DataBricks in your Data Science process (using text, diagrams, and/or tables).

DataBricks is amazing at handling really big data. DataBricks uses Spark SQL and PySpark to process its queries, and does it all in a nifty web GUI. This can be good because it handles all the messy initialization steps of setting up and using Spark SQL and PySpark. However, it can become a hassle whenever we need to download / store the notebooks, csv, parquet, or any other files to our local system.

In our projects, we have been able to use DataBricks to create our own features for all tracts in the US. For me, the easy feature was ratio of female to male and the complex feature was the estimated average house price. Trying to calculate my features in SQL, Polars, or Pandas would take around 30 minutes to an hour. I know this because I tried using sql to convert from my Idaho tract estimates to all US tracts. When I used Spark SQL and PySpark in DataBricks, the computations took about two minutes.

The only drawback to DataBricks that it is overkill for anything else other than big data. Why spend the extra effort of setting up compute, and programming the notebook in a different query language for small datasets that could easily be programmed and ran in VS Code?

2. Compare and contrast PySpark to either Pandas or the Tidyverse (using text, diagrams, and/or tables).

| Tool | Purpose | Scale | Performance | Ease of Use | Visualization
| - | - | - | - | -| - |
| PySpark | Distributed data processing on big data clusters using Apache Spark | Designed for big data, distributed computing | Scales horizontally, handles large datasets efficiently | More complex due to distributed nature, steeper learning curve | Limited native visualization, often relies on external tools like Matplotlib or Seaborn |
| Pandas | Data manipulation and analysis for smaller datasets that fit into memory | Suitable for small to medium-sized datasets | Limited by available memory, not optimized for distributed computing | 	Intuitive for data manipulation, suitable for quick analysis | Built-in visualization using Matplotlib and Seaborn

3. Explain Docker to somebody intelligent but not a tech person (using text, diagrams, and/or tables).

Docker is a way to run the contents of someone's computer to yours. It takes only the necessary parts of their computer and creates an image that can be copied and ran on your computer. This allows you to run programs that might not run on your computer because you are missing important documents. With Docker, you don't have to care about the missing documents, the program will run from the Docker image.