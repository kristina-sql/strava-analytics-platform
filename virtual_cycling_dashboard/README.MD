# Strava Virtual Cycling Analytics

A Python-based tool to extract and analyze virtual cycling data from Strava to improve cycling performance metrics, with a focus on average power and speed optimization.

## Overview

This project connects to the Strava API to automatically fetch virtual ride data, extracting key performance metrics that help cyclists track and improve their training. The extracted data is visualized in an interactive Tableau dashboard with custom Figma-designed backgrounds.

**🔗 [View Live Dashboard on Tableau Public](https://public.tableau.com/app/profile/krist.na8682/viz/ZwiftAnalyses/Overview)**

As a first API integration project, it demonstrates OAuth token management, data extraction, CSV export, and data visualization integration.

## Features

- **Automatic Token Refresh**: Manages Strava OAuth tokens automatically, refreshing them when expired
- **Virtual Ride Filtering**: Extracts only virtual rides from 2025-2026
- **Comprehensive Metrics**: Captures power, speed, heart rate, elevation, and energy expenditure data
- **CSV Export**: Saves data in an easily analyzable format
- **Tableau Integration**: Data powers an interactive dashboard with custom Figma-designed backgrounds
- **Performance Visualization**: Track trends in power output, speed, and training consistency

## Metrics Tracked

- Distance (km)
- Moving time (minutes)
- Average speed (km/h)
- Elevation gain (meters)
- Average & maximum power (watts)
- Average & maximum heart rate (bpm)
- Energy expenditure (kilojoules)

## Prerequisites

- Python 3.7 or higher
- A Strava account with recorded virtual rides
- Strava API credentials (Client ID, Client Secret)

## Installation

1. **Clone the repository**
```bash
   git clone https://github.com/yourusername/strava-virtual-cycling-analytics.git
   cd strava-virtual-cycling-analytics
```

2. **Install required packages**
```bash
   pip install -r requirements.txt
```

   Or install individually:
```bash
   pip install requests pandas python-dotenv
```

## Configuration

1. **Create a Strava API Application**
   - Go to [Strava API Settings](https://www.strava.com/settings/api)
   - Create a new application
   - Note your **Client ID** and **Client Secret**
   - Set Authorization Callback Domain to `localhost`

2. **Get Your Access Tokens**
   - Visit the following URL in your browser (replace `YOUR_CLIENT_ID`):
```
     https://www.strava.com/oauth/authorize?client_id=YOUR_CLIENT_ID&response_type=code&redirect_uri=http://localhost&scope=activity:read_all
```
   - Authorize the application
   - Copy the `code` from the redirected URL
   - Exchange the code for tokens using this curl command:
```bash
     curl -X POST https://www.strava.com/oauth/token \
       -d client_id=YOUR_CLIENT_ID \
       -d client_secret=YOUR_CLIENT_SECRET \
       -d code=YOUR_CODE \
       -d grant_type=authorization_code
```

3. **Create a `.env` file** in the project root:
```env
   STRAVA_CLIENT_ID=your_client_id
   STRAVA_CLIENT_SECRET=your_client_secret
   STRAVA_API_KEY=your_access_token
   STRAVA_REFRESH_TOKEN=your_refresh_token
   STRAVA_TOKEN_EXPIRES_AT=token_expiration_timestamp
```

## Usage

Run the extraction script:
```bash
python extract_strava_data.py
```

The script will:
1. Check if your access token needs refreshing
2. Fetch all your recent activities from Strava
3. Filter for virtual rides from 2025-2026
4. Export the data to `strava_virtual_summary.csv`

## Visualization

The extracted data is visualized in an interactive Tableau Public dashboard:

**🔗 [View the Dashboard](https://public.tableau.com/app/profile/krist.na8682/viz/ZwiftAnalyses/Overview)**

The dashboard features:
- Custom background designs created in Figma
- Performance trends over time
- Power and speed analytics
- Training volume insights
- Key performance indicators (KPIs)

To use your own data in Tableau:
1. Run the extraction script to generate your CSV
2. Upload `strava_virtual_summary.csv` to Tableau Public
3. Connect it to the dashboard template or create your own visualizations

## Output

The generated CSV file contains the following columns:

| Column | Description |
|--------|-------------|
| ride_id | Unique Strava activity ID |
| name | Activity name |
| start_date | Date and time of the ride |
| distance_km | Total distance in kilometers |
| moving_time_min | Active riding time in minutes |
| average_speed_kmh | Average speed in km/h |
| elevation_m | Total elevation gain in meters |
| average_power | Average power output in watts |
| max_power | Maximum power output in watts |
| average_hr | Average heart rate in bpm |
| max_hr | Maximum heart rate in bpm |
| kilojoules | Total energy expenditure |
| sport_type | Activity type (VirtualRide) |

## Project Structure
```
strava-virtual-cycling-analytics/
├── extract_strava_data.py    # Main extraction script
├── requirements.txt           # Python dependencies
├── .env                       # API credentials (not tracked in git)
├── .gitignore                 # Excludes sensitive files
├── strava_virtual_summary.csv # Output data file
└── README.md                  # This file
```

## Important Notes

- **Never commit your `.env` file** to GitHub - it contains sensitive credentials
- Tokens are automatically refreshed and updated in the `.env` file
- The script fetches up to 200 activities per page and handles pagination automatically
- Only virtual rides are included in the analysis

## Future Improvements

- [ ] Automate data upload to Tableau Public
- [ ] Add more virtual cycling platforms (Rouvy, TrainerRoad)
- [ ] Create power zone analysis
- [ ] Calculate training stress scores (TSS)
- [ ] Identify performance trends over time
- [ ] Compare virtual vs outdoor ride metrics
- [ ] Add weekly/monthly performance summaries
- [ ] Expand Figma design library for dashboard themes

## Troubleshooting

**"Error 401: Unauthorized"**
- Your access token may have expired - the script should refresh it automatically
- Check that your `.env` file has correct credentials

**"No virtual rides found"**
- Ensure you have virtual rides recorded in 2025 or 2026
- Check that rides are properly synced to Strava

**Import errors**
- Make sure all required packages are installed: `pip install -r requirements.txt`

## Contributing

This is a personal learning project, but suggestions and improvements are welcome! Feel free to open an issue or submit a pull request.

## License

MIT License - feel free to use this code for your own cycling analytics projects.

## Acknowledgments

- Built with the [Strava API](https://developers.strava.com/)
- Visualizations powered by [Tableau Public](https://public.tableau.com/)
- Dashboard backgrounds designed in [Figma](https://www.figma.com/)
- Created as a first API integration project to improve cycling performance

---

**Happy cycling and data analyzing! 🚴‍♀️📊**