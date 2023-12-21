# SafeGraph Semester Project

1. Please read the [Statement of Need](needs_statement.md) that your team will need to leverage to write your work proposal.
2. Use the [team_repo template](https://github.com/BYUI451/team_repo) for your writeup and submission.
    - Please make sure your [team repo](https://github.com/byuibigdata/team_repo) is private.
    - You will need to share your repo with me, `hathawayj`, and the current semester team.

## Scripts

The scripts in this template repository can help you get a picture of digesting the SafeGraph format.

### [`eda_safegraph.r`](r_examples/eda_safegraph.r)

This script depends on the Tidyverse and has two parsing functions at the top. There are some examples throughout the script of the issues with handling the SafeGraph data. The final `dat_all` object provides the full call that processes the data into a clean nested Tibble.

### [`eda_safegraph.py`](python_examples/eda_safegraph.py)

This script depends on the `safegraph_functions.py` file for some functions that can parse the nested dictionaries and lists within the POI. The Python functions create new data objects of the list and dictionary variables within the dataset.

#### [`parse_to_parquet_safegraph.py`](python_examples/parse_safegraph.py)

This file creates `.parquet` files for upload for our cloud compute.  In addition, it breaks all the nested data out into their own tables.

## SafeGraph Guides

You can see a [Colab notebook](https://colab.research.google.com/drive/1cs9qq_MWppKF4DQ0Xl3lyesHEnsc4D6D#scrollTo=_s0TsIZclcbe) that guides you through parsing data from shop.safegraph.com. As students and faculty, we get free access to the POI, patterns, and polygons data. Please register [here](https://www.safegraph.com/academics).

They filmed a series of YouTube videos which provide context for each step:

- [SafeGraph Shop Python Quickstart Part 1: Data Preparation](https://www.youtube.com/watch?v=e0X1EwBew_M)
- [SafeGraph Shop Python Quickstart Part 2: Exploding Nested JSON Fields](https://www.youtube.com/watch?v=j3A_xX7Hwqo)
- [SafeGraph Shop Python Quickstart Part 3: Joining to Census Data](https://www.youtube.com/watch?v=OQf9jCI_ltc)
- [SafeGraph Shop Python Quickstart Part 4: Scaling](https://www.youtube.com/watch?v=BvDsHJNEkU0)

Finally, check out their [documentation](https://docs.safegraph.com/docs) for an exhaustive guide to the data we are using.  Note the link to Patterns under notes that goes to the web archive.

### SafeGraph Representativeness

SafeGraph has done some work to assess how representative its sample of devices is of the entire population. Specifically, check out the *Measure and Correct Sampling Bias* section of the [Data Science Resources](https://docs.safegraph.com/v4.0/docs/data-science-resources). A recent [external audit](https://www.placekey.io/seminars/mobility-data-used-to-respond-to-covid19-could-be-biased) was done that might also be of value. The audit finds SafeGraph’s panel underrepresents older people and minorities. We hope that normalization techniques correct for some of that bias, but it is still an important consideration. [ref](https://www.safegraph.com/community/t/i-have-the-following-question-are-there-ethical-considerations-to-be-aware-of-when-using-safegraph-data-in-academic-research/5907)

### Notes

- [Download Open Census Data & Neighborhood Demographics](https://www.safegraph.com/free-data/open-census-data)
- [Search by Industry (NAICS)](https://docs.safegraph.com/reference/search-by-industry)
- [Patterns Data Dictionary](https://web.archive.org/web/20220526041151/https://docs.safegraph.com/docs/monthly-patterns)

