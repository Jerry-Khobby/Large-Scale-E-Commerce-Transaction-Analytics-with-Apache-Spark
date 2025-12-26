# PROJECT SPECIFICATION
## Large-Scale E-Commerce Transaction Analytics with Apache Spark

**Project Code:** SPARK-FAKER-001  
**Duration:** 3 Days  
**Level:** Intermediate  
**Technologies:** Python, PySpark, Faker, Parquet

## 1. PROJECT OVERVIEW

### 1.1 Purpose
Demonstrate proficiency in distributed data processing by generating and analyzing a synthetic dataset that exceeds single-machine memory limitations, requiring Apache Spark for efficient processing.

### 1.2 Learning Objectives
- Generate large-scale synthetic datasets programmatically
- Understand the limitations of Pandas for big data workloads
- Apply Spark transformations and actions for data cleaning
- Perform distributed analytics using PySpark
- Optimize Spark jobs for better performance

### 1.3 Success Criteria
- Dataset contains minimum 10 million records (target: 20-50 million)
- Dataset size exceeds 2 GB (target: 5-10 GB)
- All cleaning operations execute successfully in Spark
- Minimum 5 analytical queries completed
- Performance comparison documented (Pandas vs Spark)

---

## 2. TECHNICAL REQUIREMENTS

### 2.1 Environment Setup
```
Required Software:
- Python 3.8+
- PySpark 3.4+
- Faker 18.0+
- Pandas (for comparison only)
- Matplotlib/Seaborn (visualization)

Recommended Hardware:
- RAM: 8GB minimum (16GB recommended)
- Storage: 15GB free space
- CPU: 4+ cores
```

### 2.2 Project Structure
```
spark-faker-analytics/
│
├── data/
│   ├── raw/              # Generated data
│   ├── cleaned/          # Post-cleaning data
│   └── analytics/        # Query results
│
├── src/
│   ├── generate_data.py
│   ├── spark_cleaning.py
│   └── spark_analytics.py
│
├── notebooks/
│   └── exploration.ipynb
│
├── outputs/
│   ├── visualizations/
│   └── reports/
│
├── README.md
├── requirements.txt
└── PROJECT_REPORT.md
```

---

## 3. DATASET SPECIFICATION

### 3.1 Schema Definition

| Column Name           | Data Type | Description                    | Constraints           |
|-----------------------|-----------|--------------------------------|-----------------------|
| transaction_id        | String    | Unique transaction identifier  | NOT NULL, UNIQUE      |
| customer_id           | String    | Customer identifier            | NOT NULL              |
| customer_name         | String    | Full name                      | -                     |
| email                 | String    | Email address                  | Valid email format    |
| country               | String    | Country code (ISO)             | -                     |
| city                  | String    | City name                      | -                     |
| product_category      | String    | Product category               | Enum: 5-10 categories |
| product_name          | String    | Product description            | -                     |
| price                 | Float     | Unit price (USD)               | > 0                   |
| quantity              | Integer   | Items purchased                | > 0                   |
| total_amount          | Float     | price × quantity               | > 0                   |
| payment_method        | String    | Payment type                   | Enum: 4-5 methods     |
| status                | String    | Transaction status             | SUCCESS/FAILED        |
| transaction_timestamp | Timestamp | Transaction date and time      | Valid datetime        |

### 3.2 Data Distribution Requirements
- Date range: 2022-01-01 to 2024-12-31
- Success rate: ~85% SUCCESS, ~15% FAILED
- Payment methods: Credit Card (40%), Debit Card (30%), Mobile Money (20%), Bank Transfer (10%)
- Product categories: Electronics, Fashion, Home & Garden, Sports, Books, Beauty, Toys, Food

### 3.3 Data Quality Issues (Intentional)
Introduce realistic data quality problems:
- 3-5% missing values in non-critical fields (city, email)
- 1-2% duplicate transaction_ids
- 0.5% negative prices or quantities
- 2% records where total_amount ≠ price × quantity

---

## 4. STAGE-BY-STAGE INSTRUCTIONS

### STAGE 1: Data Generation (Day 1, 3-4 hours)

#### Objectives
- Set up Python environment
- Create data generation script
- Generate 10-50 million records
- Save in efficient format

#### Tasks
**Task 1.1:** Environment Setup
```bash
pip install pyspark faker pandas matplotlib seaborn
```

**Task 1.2:** Create `generate_data.py`
- Import Faker and required libraries
- Define function to generate single transaction record
- Implement batch generation (write in chunks of 100K-500K)
- Save as Parquet files partitioned by year-month
- Add progress logging

**Task 1.3:** Execute Generation
- Run script to generate minimum 10M records
- Monitor memory usage during generation
- Verify output files created

#### Deliverables
- `generate_data.py` script
- Raw data files in `data/raw/` (Parquet format)
- Generation log showing:
  - Total records created
  - Total file size
  - Generation time
  - Memory usage

#### Validation Checklist
- [ ] Records >= 10 million
- [ ] File size >= 2 GB
- [ ] All columns present
- [ ] Partitioned files created
- [ ] No memory errors during generation

---

### STAGE 2: Pandas Limitation Demonstration (Day 1, 1 hour)

#### Objectives
- Prove dataset exceeds Pandas capabilities
- Document memory constraints

#### Tasks
**Task 2.1:** Create `pandas_test.py`
- Attempt to load 1 million records with Pandas (should work)
- Attempt to load full dataset with Pandas
- Capture memory error or excessive load time
- Document results

**Task 2.2:** Screenshot Evidence
- Task Manager/Activity Monitor during Pandas load
- Error messages if applicable
- Load time comparison

#### Deliverables
- `pandas_test.py` script
- Screenshots of memory usage
- Written comparison in README

#### Validation Checklist
- [ ] Pandas limitation clearly demonstrated
- [ ] Memory usage documented
- [ ] Load time recorded
- [ ] Results added to README

---

### STAGE 3: Spark Session & Data Loading (Day 1, 2 hours)

#### Objectives
- Initialize Spark environment
- Load data into Spark DataFrame
- Perform initial exploration

#### Tasks
**Task 3.1:** Create `spark_cleaning.py`
- Initialize SparkSession with configuration:
  ```python
  spark = SparkSession.builder \
      .appName("TransactionAnalytics") \
      .config("spark.driver.memory", "4g") \
      .config("spark.sql.shuffle.partitions", "200") \
      .getOrCreate()
  ```

**Task 3.2:** Load Data
- Read Parquet files from `data/raw/`
- Print schema using `printSchema()`
- Display first 20 rows
- Count total records

**Task 3.3:** Initial Exploration
- Check number of partitions
- Verify data types
- Run basic `describe()` statistics

#### Deliverables
- Spark initialization code
- Schema output
- Row count confirmation
- Basic statistics output

#### Validation Checklist
- [ ] Spark session created successfully
- [ ] Data loaded without errors
- [ ] Schema matches specification
- [ ] Record count matches generation

---

### STAGE 4: Data Quality Assessment (Day 2, 2 hours)

#### Objectives
- Identify data quality issues
- Quantify problems found
- Create quality report

#### Tasks
**Task 4.1:** Null Value Analysis
```python
# Count nulls per column
# Calculate percentage of nulls
# Identify critical vs non-critical nulls
```

**Task 4.2:** Duplicate Detection
```python
# Find duplicate transaction_ids
# Count duplicates by key columns
```

**Task 4.3:** Data Validation
```python
# Identify negative prices/quantities
# Find mismatched total_amount calculations
# Check invalid date ranges
# Validate email formats (basic check)
```

**Task 4.4:** Quality Report
- Create summary DataFrame with issue counts
- Calculate quality score percentage
- Document findings

#### Deliverables
- Data quality assessment code
- Quality report (CSV or printed DataFrame)
- Summary statistics of issues

#### Validation Checklist
- [ ] Null analysis completed
- [ ] Duplicates identified
- [ ] Data validation rules applied
- [ ] Quality report generated

---

### STAGE 5: Data Cleaning (Day 2, 3 hours)

#### Objectives
- Clean identified data quality issues
- Transform data for analytics
- Save cleaned dataset

#### Tasks
**Task 5.1:** Remove Invalid Records
```python
# Filter out negative prices and quantities
# Remove critical nulls (transaction_id, customer_id)
# Remove duplicates based on transaction_id
```

**Task 5.2:** Handle Missing Values
```python
# Fill missing cities with "Unknown"
# Fill missing emails with placeholder
# Handle other nulls appropriately
```

**Task 5.3:** Data Standardization
```python
# Uppercase payment_method values
# Standardize country codes
# Trim whitespace from strings
# Correct total_amount calculations
```

**Task 5.4:** Feature Engineering
```python
# Extract: year, month, quarter, day_of_week
# Create: transaction_date (date only)
# Create: transaction_hour
# Create: is_high_value (>$500 flag)
# Create: revenue (successful transactions only)
```

**Task 5.5:** Save Cleaned Data
- Write to `data/cleaned/` as Parquet
- Partition by year and month
- Document cleaning transformations

#### Deliverables
- Complete cleaning script
- Cleaned dataset in `data/cleaned/`
- Before/after row counts
- Cleaning documentation

#### Validation Checklist
- [ ] Invalid records removed
- [ ] Missing values handled
- [ ] Data standardized
- [ ] New features created
- [ ] Cleaned data saved
- [ ] Validation queries run

---

### STAGE 6: Analytics Queries (Day 3, 4 hours)

#### Objectives
- Answer business questions using Spark
- Demonstrate aggregation capabilities
- Use Spark SQL

#### Required Analytics

**Query 1: Revenue Analysis**
```
- Total revenue by country (top 20)
- Monthly revenue trend
- Revenue by payment method
- Revenue by product category
```

**Query 2: Product Analysis**
```
- Top 20 products by revenue
- Top 20 products by quantity sold
- Average price per product category
- Products with highest failure rates
```

**Query 3: Customer Analysis**
```
- Top 50 customers by total spending
- Average order value per customer
- Customer purchase frequency distribution
- Customers with only failed transactions
```

**Query 4: Time-based Analysis**
```
- Transactions by hour of day
- Transactions by day of week
- Peak transaction months
- Success rate by quarter
```

**Query 5: Advanced Query (Window Functions)**
```
- Rank products within each category by revenue
- Calculate running total of revenue by month
- Find customers with above-average spending
```

#### Tasks
**Task 6.1:** Create `spark_analytics.py`
- Load cleaned data
- Implement each query group
- Save results to `data/analytics/`

**Task 6.2:** Use Spark SQL
- Create temporary views
- Write SQL queries for at least 2 analyses
- Compare DataFrame API vs SQL approach

**Task 6.3:** Export Results
- Save aggregated results as CSV
- Create summary statistics

#### Deliverables
- Analytics script with all queries
- Query results in `data/analytics/`
- Execution time for each query group
- SQL queries documented

#### Validation Checklist
- [ ] All 5 query groups completed
- [ ] Window function used
- [ ] Spark SQL demonstrated
- [ ] Results saved
- [ ] Execution times recorded

---

### STAGE 7: Performance Optimization (Day 3, 2 hours)

#### Objectives
- Optimize Spark job performance
- Demonstrate caching benefits
- Understand execution plans

#### Tasks
**Task 7.1:** Baseline Performance
- Choose one complex query (e.g., window function query)
- Run without optimization
- Record execution time
- Save `.explain()` output

**Task 7.2:** Apply Optimizations
```python
# Cache frequently accessed DataFrame
# Repartition data if needed
# Use broadcast join for small tables (if applicable)
# Adjust Spark configurations
```

**Task 7.3:** Compare Performance
- Re-run same query with optimizations
- Record new execution time
- Calculate improvement percentage
- Document changes made

**Task 7.4:** Parallelism Experiment
- Run query with `local[1]` (single thread)
- Run query with `local[4]` (4 threads)
- Compare execution times
- Document findings

#### Deliverables
- Performance comparison table
- Execution plan outputs
- Optimization code
- Performance report in README

#### Validation Checklist
- [ ] Baseline recorded
- [ ] Caching demonstrated
- [ ] Performance improvement shown
- [ ] Execution plans captured
- [ ] Parallelism tested

---

### STAGE 8: Visualization & Reporting (Day 3, 2 hours)

#### Objectives
- Create visualizations of key insights
- Write final project report
- Document learnings

#### Tasks
**Task 8.1:** Create Visualizations (5-6 charts)
```python
# Bar chart: Top 10 countries by revenue
# Line chart: Monthly revenue trend
# Pie chart: Revenue by payment method
# Bar chart: Top 10 products
# Histogram: Order value distribution
# Heatmap: Transactions by hour and day (optional)
```

**Task 8.2:** Write PROJECT_REPORT.md
Include:
- Executive summary
- Dataset statistics
- Data quality findings
- Key business insights
- Technical learnings
- Performance results
- Challenges faced
- Conclusion

**Task 8.3:** Update README.md
Include:
- Project description
- Setup instructions
- How to run each script
- Dataset information
- Results summary
- Technologies used

#### Deliverables
- 5-6 visualizations saved in `outputs/visualizations/`
- PROJECT_REPORT.md (2-3 pages)
- Complete README.md
- requirements.txt

#### Validation Checklist
- [ ] All visualizations created
- [ ] Report completed
- [ ] README comprehensive
- [ ] Code documented
- [ ] Requirements.txt updated

---

## 5. EVALUATION CRITERIA

### 5.1 Grading Rubric (100 points)

| Component                    | Points | Criteria                                      |
|------------------------------|--------|-----------------------------------------------|
| Data Generation              | 15     | Size, quality, schema compliance              |
| Pandas Comparison            | 10     | Clear demonstration of limitations            |
| Data Cleaning                | 20     | Thoroughness, correctness, documentation      |
| Analytics Queries            | 25     | Completeness, correctness, complexity         |
| Performance Optimization     | 15     | Demonstrated improvements, understanding      |
| Visualization & Reporting    | 10     | Quality, insights, clarity                    |
| Code Quality                 | 5      | Organization, documentation, best practices   |

### 5.2 Success Indicators
**Excellent (90-100):**
- All requirements exceeded
- Advanced Spark features used
- Clear performance gains shown
- Professional documentation

**Good (80-89):**
- All requirements met
- Solid analytics performed
- Performance optimization shown
- Good documentation

**Satisfactory (70-79):**
- Most requirements met
- Basic analytics completed
- Some optimization attempted
- Adequate documentation

---

## 6. SUBMISSION REQUIREMENTS

### 6.1 Required Files
```
✓ generate_data.py
✓ spark_cleaning.py
✓ spark_analytics.py
✓ pandas_test.py (or notebook)
✓ README.md
✓ PROJECT_REPORT.md
✓ requirements.txt
✓ Sample outputs (at least 3 query results)
✓ Visualizations (5-6 images)
```

### 6.2 Documentation Requirements
- All scripts must have docstrings
- Complex functions must be commented
- README must include setup instructions
- Report must explain technical decisions

### 6.3 Code Quality Standards
- PEP 8 compliance
- No hardcoded paths (use relative paths)
- Error handling implemented
- Logging for long-running operations

---

## 7. HELPFUL RESOURCES

### 7.1 Documentation Links
- PySpark Documentation: https://spark.apache.org/docs/latest/api/python/
- Faker Documentation: https://faker.readthedocs.io/
- Parquet Format: https://parquet.apache.org/

### 7.2 Key Spark Concepts to Understand
- Transformations vs Actions
- Lazy Evaluation
- DAG (Directed Acyclic Graph)
- Partitioning
- Caching/Persistence
- Shuffle Operations
- Catalyst Optimizer

### 7.3 Common Pitfalls to Avoid
- Don't use `.collect()` on large DataFrames
- Don't iterate row-by-row
- Don't use Pandas for final dataset
- Don't forget to stop SparkSession
- Don't ignore partition count

---

## 8. BONUS CHALLENGES (Optional)

Choose one for extra credit:

**Option A: Delta Lake Integration**
- Convert data pipeline to Delta Lake format
- Implement ACID transactions
- Show time travel capabilities

**Option B: Advanced ML**
- Build customer segmentation using K-Means
- Predict transaction failure using Spark MLlib
- Evaluate model performance

**Option C: Real-time Streaming**
- Simulate streaming data ingestion
- Use Structured Streaming
- Create real-time dashboard

**Option D: SQL Analytics**
- Create Spark SQL database
- Implement complex joins
- Build views for business users

---

## 9. PROJECT TIMELINE

### Day 1 (6-7 hours)
- Morning: Setup + Data Generation (3-4 hrs)
- Afternoon: Pandas test + Spark loading (2-3 hrs)

### Day 2 (6-7 hours)
- Morning: Data quality assessment (2 hrs)
- Afternoon: Data cleaning + feature engineering (3-4 hrs)
- Evening: Start analytics queries (1 hr)

### Day 3 (6-7 hours)
- Morning: Complete analytics queries (3 hrs)
- Afternoon: Performance optimization (2 hrs)
- Evening: Visualization + reports (2 hrs)

**Total Effort: 18-21 hours**

---

## 10. FINAL CHECKLIST

Before submission, verify:

- [ ] Dataset >= 10M records, >= 2GB
- [ ] Pandas limitation demonstrated
- [ ] All cleaning steps completed
- [ ] Minimum 5 analytical queries done
- [ ] Window function used
- [ ] Spark SQL demonstrated
- [ ] Performance optimization shown
- [ ] Caching benefit proven
- [ ] 5+ visualizations created
- [ ] PROJECT_REPORT.md completed
- [ ] README.md comprehensive
- [ ] Code properly documented
- [ ] All deliverables included
- [ ] No errors when running scripts

---

## 11. GETTING STARTED

**Step 1:** Create project directory structure
**Step 2:** Set up virtual environment
**Step 3:** Install dependencies
**Step 4:** Begin with STAGE 1
**Step 5:** Follow each stage sequentially
**Step 6:** Document as you go
**Step 7:** Test thoroughly
**Step 8:** Submit with confidence

---

**Good luck! This project will solidify your understanding of distributed data processing and give you a portfolio piece that demonstrates real-world big data skills.**

---

*Project Specification Version 1.0*  
*Last Updated: December 2025*