# EPL DataStream

## Overview
EPL DataStream is an automated Azure Function that extracts English Premier League (EPL) team and player statistics every Monday and uploads the data to Azure Blob Storage. This data can then be used for advanced analytics and visualization in tools like Tableau and Power BI.

## Architecture
The project uses an Azure Function triggered weekly to scrape the latest EPL stats from the web and upload them as CSV files to Azure Blob Storage.

![Architecture Diagram]

![image](https://github.com/jsay101/EPL_DataStream/assets/46958229/35ac9600-e70a-4d1e-8220-f6297c602966)



## Features
- Weekly data updates
- Seamless integration with visualization tools
- Data available in a clean and structured CSV format

## How to Use
### Prerequisites
- Azure account
- Azure Storage account
- Python 3.8 or later

### Setup
1. **Clone the repository:**
2. git clone (https://github.com/jsay101/EPL_DataStream)
cd epldatastream
**Install dependencies:**
pip install -r requirements.txt
3. **Set up environment variables:**
Set `AzureWebJobsStorage` to your Azure Blob Storage connection string.
4. **Deploy to Azure Functions:**
Deploy the function app to Azure. Refer to the [Azure Functions documentation](https://docs.microsoft.com/en-us/azure/azure-functions/) for detailed instructions.
