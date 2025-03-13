# Product Data Deduplication
## Task

The goal is to consolidate duplicate product entries into single, enriched records that maximize available information 
while ensuring uniqueness.

## Context

The dataset contains product details extracted from various web pages using LLMs. 
This extraction process resulted in duplicate entries where the same product appears across different sources.
Each row represents partial attributes of a product.

## Solution explanation / presentation

###### Tools : For this task, I used Python with:
* Jupyter Notebook: For proof of concept and better data visualization
* Converted into csv files for better visualization also
* Pandas: For efficient data handling and manipulation

### Approach: 


Since we know we are handling duplicate data I believe the first step should be figuring out what data types are we 
working with and creating a function to properly merge the rows.
  * Types of data
    * Strings - Preserve the longest string to retain maximum detail
    * Integer - Use the maximum value (Note: this is not ideal for `manufacturing_year`, where all values are -1). 
    If I knew how the data should look, I would implement date validation using the datetime library
    * Lists / Arrays - Concatenate unique elements into a single list 
    * None value - Replace with non-null values when available

    * ###### For the unspsc field (string type), I concatenated different values with a "|" separator to preserve all potentially important data.

  * Cleaning Data
    * column `energy_efficiency` is a dictionary, there are other columns in the dataset that contains dictionaries but 
    they are all located inside a list, in order to keep consistency over the data we are going to convert it into a list also.

#### Steps
After visualizing the data with jupyter notebook I believe the solution has to be a multistep process.

1. The first column that catches my eye is `page_url` since this has to be unique. 
We are going to first identify all the entries that have a duplicated url.

  * First problem arose, where `root_domain` is the same as `page_url`. Meaning that we can have a edge case where we
have   multiple products located on the same `root_domain`, but we identify them as duplicates.
  * In order to fix this we are going to use a combination of `page_url` and `product_title` 
(it seems to contain more details than `product_name`) as a composite key.
  
2. Second column that we are going to look at is `product_title` here we also have an edge case where the title is not 
descriptive enough. We are going to use `root_domain` again in order to make sure that there are not 2 products with the
same title on 2 different domains.

  
