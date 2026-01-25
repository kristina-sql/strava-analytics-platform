# Virtual Cycling Performance Analytics 🚴‍♂️

## Overview
End-to-end analytics pipeline analyzing Virtual (Zwift) cycling data to track training load, physiological efficiency, and performance improvements over time. Built with dbt, automated with GitHub Actions and visualized in Looker Studio.

## Key Questions This Project Answers

### 1. **Am I improving speed and power at the same physiological cost over time?**
- **Metric**: `watts_per_bpm` (Watts per Heart Rate)
- **Why it matters**: If watts/BPM increases over time, you're producing more power at the same heart rate = improved fitness
- **Visualization**: Trend line of watts_per_bpm over time

### 2. **Am I overtraining or undertraining?**
- **Metrics**: `tss_7d` (acute load) vs `tss_28d` (chronic load)
- **Why it matters**: 
  - TSS ratio > 1.5 = potential overtraining
  - TSS ratio < 0.8 = detraining risk
- **Visualization**: 7-day vs 28-day TSS trend with ratio

### 3. **Am I training in the right zones for my goals?**
- **Metrics**: `power_zone_label`, `hr_zone_label`, zone distribution
- **Why it matters**: Endurance = Z2, Threshold = Z4, different adaptations
- **Visualization**: Ride count by zone, zone progression over time

### 4. **Is my efficiency improving?**
- **Metric**: `efficiency_factor` (Normalized Power / Avg HR)
- **Why it matters**: Higher efficiency = better aerobic fitness
- **Visualization**: Efficiency factor trend with 30-day moving average

### 5. **What's my training volume and intensity pattern?**
- **Metrics**: `distance_km`, `duration_minutes`, `intensity_factor`
- **Why it matters**: Understand periodization and recovery patterns
- **Visualization**: Weekly volume bars with intensity overlay

---

## Tech Stack
- **Data Extraction**: Strava API
- **Data Warehouse**: Neon DB PostgreSql
- **Transformation**: dbt (data build tool)
- **Orchestration**: GitHub Actions (scheduled daily)
- **Visualization**: Looker Studio
- **Version Control**: Git/GitHub

---

### Architecture Overview
```
Raw Data (Strava API)
    ↓
Staging Table 
    ↓
┌─────────────────────────────────────┐
│  Star Schema (Fact + Dimension)     │
├─────────────────────────────────────┤
│ Facts:                              │
│  • fact_all_activities              │
│  • fact_hr_efficiency               │
│  • fact_activity_power_zones        │
│                                     │
│ Dimensions:                         │
│  • dim_athlete                      │
│  • dim_date                         │
│  • dim_power_zones                  │
│  • dim_hr_zones                     |
|  • dim_ftp                          │
└─────────────────────────────────────┘
    ↓
┌─────────────────────────────────────┐
│  Analytics Mart (Final Table)       │
│  virtual_cycling_summary            │
│                                     │
│  • Joins all fact & dim tables      │
│  • Daily aggregation                │
│  • Enriched with zone labels        │
│  • Rolling TSS windows (7d, 28d)    │
│  • Efficiency metrics               │
└─────────────────────────────────────┘
    ↓
Looker Studio Dashboard
```

### Table Descriptions

**Source Tables (Star Schema)**:
- `fact_all_activities` - Raw Strava activity data
- `fact_hr_efficiency` - Calculated efficiency metrics (watts/BPM, efficiency factor)
- `fact_activity_power_zones` - Power zone classification per activity
- `dim_athlete` - Athlete profile information
- `dim_date` - Date dimension for complete time series
- `dim_power_zones` - FTP-based power zone definitions (Z1-Z4)
- `dim_hr_zones` - Heart rate zone definitions (Z1-Z4)

**Analytics Mart (What Looker Uses)**:
- `virtual_cycling_summary` - **Single denormalized table** combining all source tables:
  - One row per day (including rest days with ride_count = 0)
  - All performance metrics from fact tables
  - Zone labels and benefits from dimension tables
  - Rolling 7-day and 28-day TSS calculations
  - Duration-weighted averages for fair comparison
  - Primary key: (athlete_id, activity_date)

---

## Key Metrics Glossary

### Volume Metrics
| Metric | Description | Unit |
|--------|-------------|------|
| `distance_km` | Total distance covered | Kilometers |
| `duration_minutes` | Total ride time | Minutes |
| `ride_count` | Number of rides (0 for rest days) | Count |

### Performance Metrics
| Metric | Description | Formula | Why It Matters |
|--------|-------------|---------|----------------|
| `avg_power_w_wt` | Duration-weighted average power | Σ(power × duration) / Σ(duration) | Measures sustained effort |
| `normalized_power_wt` | Duration-weighted normalized power | Σ(NP × duration) / Σ(duration) | Accounts for variable intensity |
| `avg_speed_kmh_wt` | Duration-weighted average speed | Σ(speed × duration) / Σ(duration) | Overall pace performance |
| `avg_hr_bpm_wt` | Duration-weighted average heart rate | Σ(HR × duration) / Σ(duration) | Cardiovascular effort |

### Efficiency Metrics (KEY!)
| Metric | Description | Formula | Interpretation |
|--------|-------------|---------|----------------|
| `watts_per_bpm` | Power output per heart beat | Avg Power / Avg HR | **↑ = Improved fitness** - producing more power at same HR |
| `efficiency_factor` | Aerobic efficiency | Normalized Power / Avg HR | **↑ = Better endurance** - sustainable power at lower HR |

### Training Load Metrics
| Metric | Description | Formula | Range |
|--------|-------------|---------|-------|
| `tss_total` | Training Stress Score (daily) | (Duration × NP × IF) / (FTP × 3600) × 100 | 0-500+ |
| `tss_7d` | 7-day rolling TSS (acute load) | Sum of last 7 days TSS | Monitor fatigue |
| `tss_28d` | 28-day rolling TSS (chronic load) | Sum of last 28 days TSS | Monitor fitness |
| `intensity_factor` | Ride intensity relative to FTP | Normalized Power / FTP | 0.5-1.5+ |

### Zone Classification
| Metric | Description | Values |
|--------|-------------|--------|
| `power_zone_label` | Power zone (FTP-based) | Z1 (Recovery), Z2 (Endurance), Z3 (Tempo), Z4 (Threshold) |
| `hr_zone_label` | Heart rate zone | Z1-Z4 based on max HR |
| `is_high_power_zone` | Flag for threshold+ training | 1 if Z4, 0 otherwise |
| `is_high_hr_zone` | Flag for high HR effort | 1 if HR Z4, 0 otherwise |

---

## Dashboard

**[\[Link to Looker Studio Dashboard\]](https://lookerstudio.google.com/reporting/433ef4e1-9d53-4432-bb01-5f3a14883e9e)**

### Dashboard Views:

1. **Performance Trends**
   - Watts per BPM over time (am I getting fitter?)
   - Efficiency factor trend
   - Power and speed progression

2. **Training Load Management**
   - 7-day vs 28-day TSS (am I overtraining?)
   - Daily TSS with zone intensity
   - Rest day patterns

3. **Zone Distribution**
   - Rides by power zone
   - Rides by HR zone
   - Zone time-in-zone analysis

4. **Recent Activity Log**
   - Last 10 rides with key metrics
   - Zone classification
   - Performance summary

---

## Key Insights

### Fitness Improvement
![Watts per BPM Trend](screenshots/watts_per_bpm_trend.png)
*Tracking watts/BPM shows a 12% improvement over 3 months - producing more power at the same heart rate*

### Training Load Balance
![TSS 7d vs 28d](screenshots/tss_trend.png)
*7-day TSS tracking helps prevent overtraining while maintaining progressive overload*

### Zone Distribution
![Zone Analysis](screenshots/zone_distribution.png)
*80% of rides in Z2 (endurance) with strategic Z4 (threshold) sessions for polarized training*

---

## How to Use This Project

### Prerequisites
- Neon DB account
- dbt installed (`pip install dbt-postgres`)
- Strava API credentials

### Setup
```bash
# Clone repository
git clone [your-repo-url]

# Install dependencies
dbt deps

# Run models + test
dbt build

# Generate documentation (please where from I took data lineage)
dbt docs generate
dbt docs serve
```

---

## Future Enhancements

### Phase 1: Scale to Cycling Club 
- [ ] **Multi-athlete support**: Scale database and models to handle 115+ members from my Latvian women's cycling club
- [ ] **Privacy controls**: Implement row-level security for personal data
- [ ] **Team benchmarking**: Add comparative metrics across club members (anonymized)
- [ ] **Coach dashboard**: Build summary views for training plan monitoring

### Phase 2: Granular Time-Series Analysis
- [ ] **Second-by-second data**: Analyze timing streams within activities (not just activity-level aggregates)
- [ ] **Time-in-zone analysis**: Calculate exact duration spent in each power/HR zone during a single ride

### Phase 3: AI-Powered Insights
- [ ] **Semantic layer**: Build LLM-friendly data model for natural language queries  (or integrate Gemini in Looker)
- [ ] **AI coaching assistant**: Generate personalized training suggestions based on TSS trends and zone distribution
- [ ] **Anomaly detection**: Flag potential overtraining, unusual heart rate patterns, or performance concerns
- [ ] **Recovery recommendations**: Suggest rest days based on info

### Phase 4: Women's Physiology Integration
- [ ] **Menstrual cycle tracking**: Correlate performance metrics with cycle phases
- [ ] **Nutrition logging**: Track nutrition strategies and correlation with performance
- [ ] **Private reporting**: Enhanced privacy controls for sensitive health data

### Phase 5: Outdoor Training 
- [ ] **Weather integration**: Add temperature, wind data for outdoor rides
- [ ] **Terrain analysis**: Incorporate elevation, gradient, and route difficulty
- [ ] **Indoor vs outdoor comparison**: Analyze performance differences between Zwift and road cycling
