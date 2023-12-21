# Target Engineering Challenge

For this challenge, each team will create a regression target. Each student must do their own first draft of the programming, and then a final team script must be submitted.

## Data 

Built from the Safegraph patterns data and the temples data. Results provided at the Census Tract level.

### Model Table

Your table should follow this general structure.

- __TARGET:__ The number of members actively attending church `X` distance to the nearest temple.
- __Unit of Analysis:__ We would store our data with every row representing a tract. 
    -   _Spatial:_ [CBGS](https://www.census.gov/programs-surveys/geography/about/glossary.html#par_textimage_4) or [aggregation of CBGS](https://www.census.gov/content/dam/Census/data/developers/geoareaconcepts.pdf)
   
| TRACT | target_classification | target_regression |
| ----  | --------------------- | ----------------- |
|       |  hathaway provides    |   Team provides   |

## Coding Challenge Needs

__Each tract should have the most representative number of members of The Church of Jesus Christ of Latter-day Saints that attended church using the 2019 Safegraph data.__

### Items of concern

_Note that we need to address these topics.  Some topics may not be possible, but we have to justify why we didn't or how we did handle them._

- How will you verify that you are only using standard meeting houses?
- How will you verify that your counts represent Sunday attendance?
- How will you move visitors/visits from the church location to their respective tracks?
- How will you account for visitors who aren't regular attendees of the meeting house?
- How could you leverage visitor information beyond removing it?
- How will you leverage `normalized_visits_ by_state_scaling`?
- How can we verify our estimates from 2019?
- How should we aggregate over months and days to represent members?

### Final Calculation

I will provide you with a distance to the nearest temple for each tract that you can use to get our miles traveled variable. Each student needs to do their own work. I expect to have a Pyspark coding challenge that will leverage what we do in this challenge.

### Report

_One report submitted per team in the repo. Each report should include the following._

1. an English paragraph describing the target so a non-technical user could understand.
2. A pseudocode description of how you did your calculation
3. a diagram of the tables and columns used to build the feature
4. a code snippet that demonstrates the wrangling done and the chart created
5. at least two visualizations: 1) a spatial mapping and 2) comparing the feature for tracts with and without temples
6. a display of the first five rows of your feature table used in the visualizations sorted descending by Track ID
