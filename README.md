# Product Data Deduplication

## Table of Contents
- [Task](#task)
- [Context](#context)
- [Solution explanation / presentation](#solution-explanation--presentation)
  - [Tools](#tools)
  - [Approach](#approach)
    - [Understanding the data and cleaning it (featuring a plot twist)](#understanding-the-data-and-cleaning-it-featuring-a-plot-twist)
    - [Possibility of removing some columns](#possibility-of-removing-some-columns)
    - [How do we merge rows?](#how-do-we-merge-rows)
    - [ValueErrors. Why do we raise them and how do we handle them](#valueerrors-why-do-we-raise-them-and-how-do-we-handle-them)
    - [Merging rows](#merging-rows)
- [Output](#output)
  - [Challenges](#challenges)
  - [Solution](#solution)
  - [Where to Find the Final Dataset](#where-to-find-the-final-dataset)
- [Food for thought](#food-for-thought)
- [The Fun Part: Data About Our Data](#the-fun-part)
  - [How did we optimize the processing](#how-did-we-optimize-the-processing)

  

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
#### Understanding the data and cleaning it (featuring a plot twist)

#### Mistakes were made early:

* **Initial assumption:** I approached this as a vendor-centric problem, treating products from different domains as
different entities that shouldn't be merged. Going as far as raising and catching `ValueError` exceptions to maintain
data integrity across domain boundaries.

* **The reality:** After re-reading the task description more carefully, I realized this is actually a product-focused challenge. 
The goal is to consolidate identical products across different sources into unified, enriched entries.

* **The fix:** Thankfully, my code was structured modular enough that pivoting required minimal changes.
I adjusted the merging logic for `root_domain` and `page_url`, along with changing the duplicate detection logic.

Sadly we are lacking enough information about the dataset and, its usage so below are more like educated guesses of
what should we do rather than an actual "best-in-case" approach

* Column `energy_efficiency` is a dictionary, there are other columns in the dataset that contains dictionaries however 
they are all located inside a list, in order to keep consistency over the data, and avoid complex logic to concatenate 
dictionaries we are going to convert it into a list also.

#### Possibility of removing some columns

Since one of the factors for this challenge is scalability we can consider removing some of the columns to improve memory
footprint and maybe even processing speed. I will also try my best to consider data integrity, possible data usages and 
possible future data requirements into my analysis.

* Columns `product_summary` and `description` using cosine similarity algorithm we determined a 69.51% similarity between
the two columns. With this in mind I decided to merge the 2 columns into one by keeping the longest string
assuming it provides the most details about a product.
  * New column : `product_description`, both initial columns dropped.


* Columns `product_name` and `product_title` using cosine similarity algorithm we determined a 71.54% similarity between
the 2 columns, for this specific case I decided to keep the product_title since it appears to provide a better description
of the product.
  * Kept column : `product_title` , dropped :`product_name`.


* Column `manufacturing_year` in our dataset has only one value `-1` across the entire set, with the assumption that 
this is basically the equivalent of `None` I assume the best approach would be to delete, the only argument for keeping 
it would be "future proofing".
  * Dropped column : `manufacturing_year`.


* Columns `materials` and `ingredients` these two columns have the same data type, and seem to be a pretty similar "description"
of the product, with the only difference being that one is edible and one is not. Normally I would keep this two columns
as is since there is a clear distinction between the data, however since we don't know the usage of the dataset I decided
to merge them into a single column.
  * After checking for rows that contain information for in both columns we identified 0 such cases. Meaning we could 
merge the columns and save a bit of space. 
  * New column : `components`, both initial columns dropped.  

###### Some things are better left untouched

* Columns `purity`, `pressure_rating` and `power_rating` they are pretty similar with the above case, the interesting aspect
about these columns is that they all store dictionaries with the same keys. I planned to keep them as they were, 
but since this is a challenge and the data structure allows, I decided to merge them and see what we could break.
* Later edit – We _**did**_ break things. Even though all three columns share the same dictionary keys, the purity column 
never contains a value for `key:unit`. Since `pressure_rating` and `power_rating` also occasionally lack a unit value,
merging them results in ambiguous data.
  
###### Possible solutions:
  1. Assigning a descriptive value based on what column it came from for each instance of `key:unit - value:None`:
* Example: `key:unit - value:power` to indicate which column the data originated from.
* Downside: This still might not fully resolve ambiguity.

  2. Using a Nested Dictionary Format
* Example: `[{"source": "power_rating", "data": {"qualitative": false ..."}}]`.
* Downside: More complex structure
  
* Results: After testing the second approach, we only managed to reduce the file size by 0.17% (14,580 bytes), from 7.97 MB to 7.96 MB.

* **Given the minimal impact on file size and the added complexity, it’s not worth merging these columns. 
Keeping them separate ensures data clarity.**

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

* Columns `root_domain` and `page_url`, for these columns the decision of what we do with data is rather tough:
  1. Preserve all unique entries with a separator ` | ` between them, in order to preserve the `dtype=string` from 
original data.
  2. Prioritize one of them based on something like: availability of the product, but without more information this seems
like a bad idea.
  3. Preserve only page_url since root_domain can be rather easily extracted from it.
  4. Preserve all unique `root_domain`, and for `page_url` we only preserve the shortest string for each domain. This 
is probably the best option since it keeps essential information while removing tracking parameters, session IDs, 
and other dynamic elements that don't represent the core product URL.


| Data Type                            | Merging Strategy                                                                                                                            |
|--------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------|
| **strings**                          | Keep the longest string to retain maximum detail. Unless otherwise specified below.                                                         |
| **unspsc<br/>(str)**                 | Concatenated different values with " \| " separator to preserve all potentially important data.                                             |                                                                                                                                  
| **root_domain<br/>(str)**            | Concatenated different values with " \| " .                                                                                                 |
| **page_url<br/>(str)**               | Concatenated different values with " \| " .                                                                                                 |
| **eco_friendly<br/>(bool)**          | None and True → `True`, <br/>None and False → `False`, <br/>True and False → raise `ValueError`.                                            |
| ~~**manufacturing_year<br/>(int)**~~ | ~~Use the maximum value, not ideal.<br/>If I knew how the data should look, I would implement date validation using the datetime library.~~ |
| **lists**                            | Concatenate all unique elements into a single list.                                                                                         |
| **dicts**                            | Concatenate all unique dictionaries as elements of a single list.                                                                           |
| **none values**                      | Replace with non-null values when available.                                                                                                |
                                                                                   
#### ValueErrors. Why do we raise them and how do we handle them


In this solution we have a functions that raise `ValueError`: `merge_eco_friendly`.
In this function we use ValueError to catch and handle cases where rows should not be merged due to logical
inconsistencies or data conflicts.

It's worth mentioning column `brand`, I considered raising a `ValueError` here also since at the first glance it doesn't
make much sense that the same product have multiple brands, however after checking the logged conflicts 
(that were quite numerous, around 600 rows). I've decided that the best approach is to merge this column by longest string.

By raising this ValueError exceptions, we preserve data integrity and ensure that the merging process only takes place 
when it's logically valid. Errors are caught within the `merge_dataframe_rows` function, which logs the conflicting data 
into a separate CSV file. This allows the function to continue processing and merging the non-conflicting rows without 
interruption.

#### Merging rows

We are merging rows with product_title as the key column. I've considered creating custom composite keys from multiple
columns for more precise matching, but after switching to a product-centric approach, product_title proved to be the most
reliable single identifier.

Custom keys (like combining brand+product_title) were explored but ultimately rejected because they introduced false negatives
by over-segmenting what should be considered the same product

## Output


### Challenges

Since the initial data was provided as a `.snappy.parquet` file, I figured the output should be same type of file.

When saving our processed DataFrame using pandas' `df.to_parquet()` method (which uses the PyArrow engine), 
we encountered several limitations:

1. **Complex Nested Data Structures**: Columns containing dictionaries inside lists caused serialization errors,
as PyArrow has limited support for deeply nested data structures.

2. **Mixed Data Types**: Columns with inconsistent data types across rows couldn't be properly serialized.

3. **Type Conversion Issues**: 
   - Simply using Pandas built in method `.astype(object)` didn't resolve the serialization problems.
   - Converting to strings `.astype(string)` created an interesting problem. The framework was allocating memory for each 
string creating multiple dtypes based on memory allocation inside our column. Preventing using pyarrow engine to save the data.
   

### Solution:
   - For simple lists containing only strings, we used the native `Python list `
   - For complex structures (lists containing dictionaries),  we turned each dictionary into a JSON string using `json.dumps()`.
After data processing we ended up saving the string inside a `Python list`


### Where to Find the Final Dataset

The cleaned and processed dataset is available in the Release folder of this repository. 
You can find it in [Release](https://github.com/ImperaLuna/veridion_product_deduplication_challenge/releases/tag/v1.0.0).


## Food for thought


The `description` and `product_summary` columns contain potentially valuable information about products that aren't captured elsewhere.
In some cases, these text fields include critical identifiers (like missing product numbers or manufacturing years)
that could improve merging decisions.

**Possible Advanced Solutions:**
1. Pattern extraction using regular expressions to identify product codes, years, etc
2. Natural language processing (NLP) techniques to analyze semantic similarity between product descriptions
3. Named entity recognition to extract and compare specific product attributes mentioned in descriptions

While implementing these approaches could potentially improve merging accuracy, they would require significant additional
development time. After careful consideration, I determined these enhancements were beyond the scope of the current 
challenge and have documented this as an opportunity for future improvement.

<h2 id="the-fun-part">The Fun Part: Data About Our Data <br/><span style="font-size: 0.9rem; font-weight: bold;">(a.k.a. Numbers That Make Us Feel Productive)</span></h2>
###### DataFrame Comparison Summary

| Metric              | Original | Final  | Difference | Percentage |
|---------------------|----------|--------|------------|------------|
| Rows                | 21,946   | 18,954 | -2,992     | -13.63%    |
| Columns             | 31       | 27     | -4         | -12.90%    |
| Size (MB)           | 10.72    | 7.24   | -3.48      | -32.48%    |
| Processing Time (s) | 16.24    | 0.61   | -15.63     |  96.24%    |


### How did we optimize the processing


The optimized code improves performance by implementing a "process only what's necessary" approach.
Instead of running expensive merge operations on the entire dataset, it first identifies which rows actually have duplicates
and only processes those specific subsets.

Additionally, I learned that utilizing `.copy()` method is better for memory efficiency rather than making operations
in-place. Initially I believed that avoiding copying the dataframe would save on memory. However, it seems like my 
assumption was wrong, Pandas is not only creating internal copies anyways, but also forcing reindexing operations.






