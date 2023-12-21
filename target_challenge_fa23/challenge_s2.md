# Target Engineering Challenge

A final target table submission.

## Data 

Built from the Idaho Safegraph patterns data and the temples data. Results provided at the Census Tract level. __Each tract should have the most representative number of members of The Church of Jesus Christ of Latter-day Saints that attended church using the 2019 Safegraph data.__

### Model Data

Your file should have the same number of rows as the `.parquet` file in the [nearest_temple](nearest_temple) folder.

- __active_member:__ The number of members actively attending church `X` distance to the nearest temple.
- __Unit of Analysis:__ We would store our data with every row representing a tract. 
   
| TRACT      | active_member         |
| ---------  | --------------------- |
|  4xxx4     |  1243                 |
|  ....      |  ....                 |
|  5xxx8     |  1921                 |


### IPYNB Format

Please mimic the outputs shown (as close as possible) in the `.ipynb` file in [nearest_temple](nearest_temple). Specifically, please show the mean, count, standard deviation, min, max, and quartiles.  Additionally, show your table for the tracts identified in the file.

```python
rexburg_tracts = ["16065950100", "16065950200", "16065950400", "16065950301", "16065950500", "16065950302"]
courd_tracts = ["16055000402", "16055000401", "16055001200", "16055000900"]
```

#### Visualizations

In your `.ipynb` file please include the following.

1. Plot your calculations against at least one other group's calculations and discuss the differences.
2. Plot a histogram of active counts
3. Plot a histogram of the percent of the tract that is active

## Repository Push

Please name your `.ipynb` file and your `.paruqet` file the same name and include it in the [data](data) folder.  Each pull request should only have two files in the folder.
