# UFC Winner Predictor
This project is in **Wave 1**, meaning it focuses on using historical performance stats (career + recent fights) to estimate win probabilities of any given matchup. Future waves will add more features such as betting market data, age, cardio comparisons, reach, etc...

Currently, it's an end-to-end pipeline: **scraping UFC fight data** → building a **PostgreSQL dataset** → engineering fighter/matchup features → training a **baseline machine learning model** to predict fight winners.

## Features
- **Web Scraping from UFCStats**
    - Scrapes completed UFC events from ufcstats.com
    - Parses:
        - Fighters (IDs + names)
        - Fights (event, date, weight class, method, round/time, basic winner info)
        - Per-fighter fight stats (KD, significant/total strikes, takedowns, subs, control time, fight duration)
- **PostgreSQL Data Model**
    - `schema.sql` defines 3 tables
        - `fighters` - 1 row per fighter
        - `fights` - 1 row per fight (metadata + winner)
        - `fighter_stats` - 1 row per fighter per fight (striking + grappling stats, time fought)
    - All ingestion done with SQLAlchemy + pandas
- **Feature Engineering**
    - Builds a `fighter_features` table with:
        - Career-level aggregates (all recorded UFC fights)
        - Last-3 fights aggregates (recency bias)
    - Example features:
        - `career_fights_count`, `career_wins_count`, `career_win_rate`
        - `career_sig_strikes_per_min`, `career_td_accuracy`
        - Matching `last3_`... versions for recent form
- **Matchup Dataset**
    - Joins fighter features for both fighters in each completed fight
    - Produces a `fight_matchups` table with:
        - Fighter1 + Fighter2 feature columns (`f1_*`, `f2_*`)
        - Difference features (`diff_* = f1_feature - f2_feature`)
        - Label `f1_win` (1 if fighter1 won, 0 otherwise)
- **Baseline ML Model**
    - Logistic regression trained on `diff_*` features
    - Handles class imbalance by mirroring matchups:
        - For each fight, creates a flipped version (swap fighters and invert labels)
    - Saves model as `models/baseline_logreg.pkl` (includes classifier and expected feature column list)
- **CLI Prediction Tool**
    - `scripts/predict_upcoming.py`:
        - Takes two fighters (by name / fighter_id)
        - Looks up their features in the DB
        - Builds the same `diff_*` feature vector the model expects
        - Prints `P(Fighter 1 wins)` and the predicted winner

## Tech Stack
- **Language**: Python (tested with 3.12)
- **Data**: PostgreSQL, SQLAlchemy, pandas
- **Scraping**: requests, beautifulsoup4
- **ML**: scikit-learn (logistic regression, train/test split, basic metrics)
- **Config**: .env + python-dotenv (DB URL, base URLs)

## Setting Up the Environment
### 1. Clone and Set Up the Virtual Environment
```bash
git clone https://github.com/a-bakhtine/ufc-predictor.git
cd ufc-predictor

python3 -m venv .venv
source .venv/bin/activate   

pip install -r requirements.txt
```

### 2. PostgreSQL Setup
1. Create a Postgres database, i.e.:
    ```bash
    createdb ufc_db
    ```
2. Create tables using `schema.sql`:
    ```bash
    psql ufc_db < schema.sql
    ```
3. Configure the connection string:
    ```bash
    cp .env.example .env
    ```
    Then, edit `.env` accordingly (enter your username and password)


## How To Use This Tool
### 1. Ingest Data from UFCStats
Scrape recent completed events and load them into Postgres:
```bash
python3 scripts/etl_ufcstats.py
```
This populates the tables `fighters`, `fights`, and `fighter_stats`

### 2. Build Fighter Features
```bash
python3 scripts/compute_features.py
```
Creates `fighter_features` with career + last-3 stats per fighter.

### 3. Build Matchup Dataset
```bash
python3 scripts/compute_matchups.py
```
Creates `fight_matchups` with `f1_*`, `f2_*` features, `diff_*` features, `f1_win` label

### 4. Train the Baseline Model
```bash
python3 scripts/train_baseline_model.py
```
Trains a logistic regression model and saves it to `models/baseline_logreg.pkl`

### 5. Predict a Matchup
```bash
python3 scripts/predict_upcoming.py "Islam Makhachev" "Jack Della Maddalena"
```
Outputs the predicted win probability for fighter 1 and the predicted winner (example below)
```
================ UFC Matchup Prediction ================

Fighter 1: Islam Makhachev (fighter_id=275aca31f61ba28c)
Fighter 2: Jack Della Maddalena (fighter_id=6b453bc35a823c3f)

Model predicts P(Fighter 1 wins) = 0.672

Predicted winner: Islam Makhachev

(Interpretation is based currently on career and last 3 stats.)
```
 ***Disclaimer:*** *This project is for learning, fun, and research. It is* ***not*** *financial or betting advice.*