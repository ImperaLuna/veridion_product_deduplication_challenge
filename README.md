# Product Data Deduplication
## Task

The goal is to consolidate duplicate product entries into single, enriched records that maximize available information 
while ensuring uniqueness.

## Context

The dataset contains product details extracted from various web pages using LLMs. 
This extraction process resulted in duplicate entries where the same product appears across different sources.
Each row represents partial attributes of a product.

## Solution explanation / presentation

Just a heads-up: this isn't a step-by-step solution, but more like a battle log from the trenches of problem-solving
(cleaned up just enough so readers don’t lose their minds).

### Tools : For this task, I used Python with:
* Jupyter Notebook: For proof of concept and data visualization
* Csv files for better visualization
* Pandas: For efficient data handling and manipulation

### Approach:
#### Understanding the data and cleaning it

Sadly we are lacking enough information about the dataset and, its usage so below are more like educated guesses of
what should we do rather than an actual "best-in-case" approach

* Column `energy_efficiency` is a dictionary, there are other columns in the dataset that contains dictionaries however 
they are all located inside a list, in order to keep consistency over the data, and avoid complex logic to concatenate 
dictionaries we are going to convert it into a list also. - Later edit : this is no longer required since I created 
robust solution for merging dictionary arrays but this will be kept for data consistency.

#### Possibility of removing some columns

Since one of the factors for this challenge is scalability we can consider removing some of the columns to improve memory
footprint and maybe even processing speed. I will also try my best to consider data integrity, possible data usages and 
possible future data requirements into my analysis.

* Columns `root_domain` and `page_url`, since `page_url` is a more descriptive for a product and `root_domain` can be 
pretty easily extracted from `page_url` I considered dropping `root_domain`. However, it ended up remaining on the dataset
because it's a good column to check for products with the same name on multiple domains. 

* Columns `product_summary` and `description` using cosine similarity algorithm we determined a 69.51% similarity between
the two columns. With all of the above in mind I decided to merge the 2 columns into one by keeping the longest string
assuming it provides the most details about a product.

* Columns `product_name` and `product_title` using cosine similarity algorithm we determined a 71.54% similarity between
the 2 columns, for this specific case I decided to keep the product_title since it appears to provide a better description
of the product.

* Column `manufacturing_year` in our dataset has only one value `-1`, with the assumption that this is basically the equivalent
of `None` I assume the best approach would be to delete.

* Columns `materials` and `ingredients` these two columns have the same data type, and seem to be a pretty similar "description"
of the product, with the only difference being that one is edible and one is not :). However since we don't know the usage
of the dataset and there seem to be a clear distinction between them, I decided the best approach is to keep them both. 

* Columns `purity`, `pressure_rating` and `power_rating` they are pretty similar with the above case, the interesting aspect
about these columns is that they all contain dictionaries with the same keys. I planned to keep them as they were, 
but since this is a challenge and the data structure allows, I decided to merge them and see what I could break. :D.
  * Later edit – We _**did**_ break things. Even though all three columns share the same dictionary keys, the purity column 
never contains a value for `key:unit`. Since `pressure_rating` and `power_rating` also occasionally lack a unit value,
merging them results in ambiguous data. 

#### How do we merge rows?

Since we know we are handling duplicate data I believe the first step should be figuring out what data types are we 
working with and creating a function to properly merge the rows.

* **Analysing Arrays**  
  After analyzing the data, we can divide the array columns into two entities: simple arrays containing only `strings`
and arrays containing `dictionaries`.

* **Merging Dictionaries**  
  After careful consideration I determined the best way to merge multiple entries that have dictionaries is to add all 
the unique ones into the list. The downside to this approach is that we might add extra 
size to our dataset, but in return, we maintain data integrity.


| Data Type                            | Merging Strategy                                                                                                                            |
|--------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------|
| **strings**                          | Keep the longest string to retain maximum detail. Unless otherwise specified below.                                                         |
| **root_domain<br/>(str)**            | if non unique → raise `ValueError`.                                                                                                         |
| **page_url<br/>(str)**               | Keep the shortest string for clarity and redundancy reduction.                                                                              |
| **unspsc<br/>(str)**                 | Concatenated different values with " \| " separator to preserve all potentially important data.                                             |                                                                                                                                  |
| **eco_friendly<br/>(bool)**          | None and True → `True`, <br/>None and False → `False`, <br/>True and False → raise `ValueError`.                                            |
| ~~**manufacturing_year<br/>(int)**~~ | ~~Use the maximum value, not ideal.<br/>If I knew how the data should look, I would implement date validation using the datetime library.~~ |
| **lists**                            | Concatenate unique elements into a single list.                                                                                             |
| **dicts**                            | Concatenate all unique dictionaries into a single list.                                                                                     |
| **none values**                      | Replace with non-null values when available.                                                                                                |
                                                                                   
#### ValueErrors. Why do we raise them and how do we handle them


In this solution, there are two functions that raise `ValueError`: `merge_eco_friendly` and `merge_root_domain`. 
Both of these functions use ValueError to catch and handle cases where rows should not be merged due to logical
inconsistencies or data conflicts.

`merge_eco_friendly`: This function handles the merging of rows based on the eco_friendly column, which is of
boolean type. The simplicity of the eco_friendly column makes it ideal for error handling and assuring that only
compatible rows are merged. 

`merge_root_domain`: After creating merge_eco_friendly, I realized that merging products from different root_domains makes 
no logical sense. Therefore, I introduced a ValueError in merge_root_domain to prevent the merging of products 
from different domains.

By raising these ValueError exceptions, we preserve data integrity and ensure that the merging process only takes place 
when it's logically valid. Errors are caught within the merge_dataframe_rows function, which logs the conflicting data 
into a separate CSV file. This allows the function to continue processing and merging the non-conflicting rows without 
interruption.

#### Steps for merging rows
After visualizing the data with jupyter notebook I believe the solution has to be a multistep process.

1. The first column that catches my eye is `page_url` since this has to be unique. 
We are going to first identify all the entries that have a duplicated url.

  * First problem arose, where `root_domain` is the same as `page_url`. Meaning that we can have an edge case where we
have   multiple products located on the same `root_domain`, but we identify them as duplicates.
  * In order to fix this we are going to use a combination of `page_url` and `product_title` 
(it seems to contain more details than `product_name`) as a composite key.
  
2. Second column that we are going to look at is `product_title` here we also have an edge case where the title might not be
descriptive enough. We are going to use `root_domain` again in order to make sure that there are not 2 products with the
same title on 2 different domains.


#### What do we take home 

parquet, numpy and data types

#### Food for thought

Description and how it can be used to determine non duplicate rows

### The Fun Part: Data About Our Data <br/>(a.k.a. Numbers That Make Us Feel Productive)


###### DataFrame Comparison Summary

| Metric    | Original | Final    | Difference   | Percentage   |
|-----------|----------|----------|--------------|--------------|
| Rows      | 21,946   | 19,054   | -2,892       | -13.18%      |
| Columns   | 31       | 28       | -3           | -9.68%       |
| Size (MB) | 10.72    | 6.79     | -3.93        | -36.70%      |



### Where to Find the Final Dataset

The cleaned and processed dataset is available in the Release folder of this repository. You can find it in[https://github.com/ImperaLuna/veridion_product_deduplication_challenge/releases/tag/v1.0.0](Release).

