# 🚀 Automated Data Pipeline for Report Analytics

A robust and automated ETL (Extract, Transform, Load) pipeline built with Python and SQL. This project efficiently extracts operational data from a Microsoft SQL Server, processes it, identifies changes, and loads it into a local SQLite data warehouse. The final, analytics-ready dataset is exported to CSV and delivered to SharePoint.

This solution solves the critical business problem of providing timely and accurate data for reporting and business intelligence, without manual intervention.



## 🏛️ Project Architecture

The pipeline follows a modern data engineering workflow, ensuring data is processed efficiently and reliably from source to destination.

<img width="639" height="215" alt="image" src="https://github.com/user-attachments/assets/d6a9cf6b-7937-458c-a709-5745270e1ec0" />



**The data flows through the following stages:**
1.  **Scheduled Trigger:** The process is initiated by a scheduler (e.g., every 10 minutes).
2.  **Data Staging (MS SQL Server):** A stored procedure (`dbo.data_Refresh_test`) runs on the source database, transforming raw data into an analytics-friendly format in a staging table.
3.  **Extraction (Python):** A Python service connects to the MS SQL Server and extracts the staged data in manageable chunks to conserve memory.
4.  **Transformation & CDC (Python):** Each row is hashed to create a unique signature. This hash is used for Change Data Capture (CDC) to identify new or updated records.
5.  **Load (SQLite):** The script performs an "upsert" operation into a local SQLite database. New records are inserted, and existing records are updated only if their hash has changed.
6.  **Delivery (CSV & SharePoint):** The updated data from SQLite is exported to a CSV file and automatically uploaded to a designated SharePoint folder, making it available for consumption.
7.  **Logging:** Every step of the process is logged, capturing metrics like records processed and job status for monitoring and debugging.
```bash
  "f2808f26-391f-4842-9f27-b2c6f0081b87": {
            "status": "complete",
            "message": "Full sync finished.",
            "total_rows_received": 113953,
            "total_rows_updated_in_cache": 86564,
            "exported_file": "C:\\Users\\panch-hr\\source\\repos\\fast-api\\final_project\\exports\\etl_master_export.xlsx",
            "start_time": "2025-08-07 14:27:39.815778",
            "end_time": "2025-08-07 14:28:24.491988"
        },
        "3f5b7575-ad30-4ff2-b379-402cce3cf271": {
            "status": "complete",
            "message": "Full sync finished.",
            "total_rows_received": 113953,
            "total_rows_updated_in_cache": 86564,
            "exported_file": "C:\\Users\\panch-hr\\source\\repos\\fast-api\\final_project\\exports\\etl_master_export.xlsx",
            "start_time": "2025-08-07 14:42:39.746860",
            "end_time": "2025-08-07 14:43:11.411998"
        }
```


## ✨ Key Features

* **Efficient Change Data Capture (CDC):** Uses row hashing to process only new or modified data, significantly reducing load times and computational overhead.
* **Modular & Maintainable:** Business logic is encapsulated in a SQL stored procedure, separating it from the Python orchestration code for easy updates.
* **Scalable & Performant:** Processes data in configurable chunks, ensuring the application can handle large datasets without running out of memory.
* **Fully Automated:** The entire pipeline runs on a schedule, providing a hands-off solution for data delivery.
* **Configurable:** Key parameters like database credentials, chunk size, and schedule intervals can be easily managed through an environment file.
* **Robust Logging:** Comprehensive logging provides visibility into every job run, making it easy to monitor and troubleshoot.



## 💻 Technology Stack

* **Backend:** Python
* **API Framework:** FastAPI, Uvicorn
* **Database ORM:** SQLAlchemy
* **Source Database:** Microsoft SQL Server
* **Data Warehouse:** SQLite
* **DB Driver:** pyodbc
* **Data Manipulation:** Pandas
* **Configuration:** python-dotenv
* **File Handling:** openpyxl (for potential Excel operations)


## ⚙️ Setup and Installation

### Prerequisites

* Python 3.8+
* Access to a Microsoft SQL Server instance
* Microsoft ODBC Driver for SQL Server
* A SharePoint site for final data delivery

### Installation Guide

1.  **Clone the repository:**
    ```bash
    git clone [Link to your GitHub repository]
    cd [your-project-directory]
    ```

2.  **Create and activate a virtual environment:**
    ```bash
    # For Windows
    python -m venv venv
    .\venv\Scripts\activate

    # For macOS/Linux
    python3 -m venv venv
    source venv/bin/activate
    ```

3.  **Install dependencies:**
    *(**Note:** First, make sure you have a `requirements.txt` file by running `pip freeze > requirements.txt` in your activated environment.)*
    ```bash
    pip install -r requirements.txt
    ```


## 🚀 Usage
To start the automated pipeline, run the main application using Uvicorn:

The server will start, and the ETL process will be triggered at the interval defined in your .env file.
<img width="956" height="479" alt="image" src="https://github.com/user-attachments/assets/c3b2a3d5-839e-4f37-ac69-c4525f37a571" />

## 📄 License
This project is distributed under the MIT License. See the LICENSE file for more information.

