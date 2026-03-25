# Shutdown Rating System

## Overview
This is a web-based shutdown rating system built using Flask and deployed on Azure App Service.

Users can submit ratings through a web form. The data is automatically written back to SharePoint and can then be consumed by OPMS for further processing and reporting.

Architecture
User → Web Application (Flask) → Data Processing → SharePoint → OPMS

The application is hosted on Azure App Service and deployed automatically using GitHub Actions.

## Features
- Web-based rating form
- Automatic data processing
- Direct integration with SharePoint
- Cloud deployment on Azure
- Automated deployment via GitHub Actions
- Ready for OPMS integration

## Technology Stack
- Python (Flask)
- Azure App Service
- GitHub Actions
- SharePoint API
- pandas, requests, openpyxl

## Project Structure
- Web.py (main application)
- SharepointAPI.py (SharePoint integration)
- Datacleaning.py (data processing)
- templates/ (HTML files)
- static/ (CSS and assets)
- requirements.txt (dependencies)

## Data Flow
1. User submits data through the web form
2. The backend processes the data
3. Data is written to SharePoint
4. Data is available for OPMS

## Notes
- The application loads the latest data when opened
- Designed for internal operational use
- Can be extended for additional workflows
