#evaluates multiple model types and finds the best
#one for data given MSE
import pandas as pd
import sqlalchemy
import os
from sqlalchemy import create_engine
import re
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_squared_error, mean_absolute_error
from sklearn.preprocessing import StandardScaler
import joblib
import logging
import matplotlib.pyplot as plt
from sklearn.linear_model import LinearRegression
from sklearn.ensemble import GradientBoostingRegressor
from xgboost import XGBRegressor
from sklearn.svm import SVR

# Enable logging
logging.basicConfig(level=logging.INFO)

# Database connection
db_password = os.environ.get('DB_PASSWORD')
engine = create_engine(f'postgresql+psycopg2://postgres:{db_password}@localhost:5432/Tier1')

# Query to load data
query = "SELECT * FROM public.read_shot_events WHERE league = 'USA-MajorLeagueSoccer'"

logging.info("Executing query")
df_shot_events = pd.read_sql_query(query, engine)
logging.info("Query executed successfully")

# Drop irrelevant columns
df_shot_events.drop(columns=['notes', 'body_part', 'minute', 'league', 'season', 'player', 'rsexG', 'distance',
                             'SCA 1player', 'SCA 1event', 'SCA 2player', 'SCA 2event'], errors='ignore', inplace=True)

# Remove the date from the game column and split the game column to get home and away teams
df_shot_events['game_info'] = df_shot_events['game'].apply(lambda x: x.split(' ', 1)[1])
df_shot_events[['home_team', 'away_team']] = df_shot_events['game_info'].str.split('-', expand=True)

# Clean up any extra whitespace around team names
df_shot_events['home_team'] = df_shot_events['home_team'].str.strip()
df_shot_events['away_team'] = df_shot_events['away_team'].str.strip()

# Aggregate goals per game
df_shot_events['home_goal'] = df_shot_events.apply(lambda row: 1 if row['outcome'] == 'Goal' and row['team'] == row['home_team'] else 0, axis=1)
df_shot_events['away_goal'] = df_shot_events.apply(lambda row: 1 if row['outcome'] == 'Goal' and row['team'] == row['away_team'] else 0, axis=1)

# Group by game and sum goals and PSxG
aggregated = df_shot_events.groupby('game').agg({
    'home_goal': 'sum',
    'away_goal': 'sum',
    'rsePSxG': 'sum'
}).reset_index()

# Split aggregated data back into home and away PSxG
aggregated['home_psxg_avg'] = aggregated['rsePSxG'] / df_shot_events.groupby('game')['home_team'].transform('count')
aggregated['away_psxg_avg'] = aggregated['rsePSxG'] / df_shot_events.groupby('game')['away_team'].transform('count')

# Drop the combined PSxG column
aggregated.drop(columns=['rsePSxG'], inplace=True)

# Rename columns for clarity
aggregated.rename(columns={'home_goal': 'total_home_goals', 'away_goal': 'total_away_goals'}, inplace=True)

# Select features for training
features_home = ['home_psxg_avg']
features_away = ['away_psxg_avg']

X_home = aggregated[features_home]
y_home = aggregated['total_home_goals']

X_away = aggregated[features_away]
y_away = aggregated['total_away_goals']

# Print the final DataFrame
print(aggregated.head())
print(aggregated.tail())
print(aggregated.columns)

# Function to train and evaluate multiple models
def train_and_evaluate_models(X, y):
    models = {
        'Linear Regression': LinearRegression(),
        'Gradient Boosting Regressor': GradientBoostingRegressor(random_state=42),
        'XGBoost Regressor': XGBRegressor(random_state=42),
        'Support Vector Regressor': SVR(),
        'Random Forest Regressor': RandomForestRegressor(random_state=42)
    }
    
    results = {}
    
    for model_name, model in models.items():
        logging.info(f"Training model: {model_name}")
        
        # Split the data into training and testing sets
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
        
        # Scale the features
        scaler = StandardScaler()
        X_train_scaled = scaler.fit_transform(X_train)
        X_test_scaled = scaler.transform(X_test)
        
        # Train the model
        model.fit(X_train_scaled, y_train)
        
        # Make predictions
        y_pred = model.predict(X_test_scaled)
        
        # Evaluate the model
        mse = mean_squared_error(y_test, y_pred)
        results[model_name] = mse
        logging.info(f'{model_name} - Mean Squared Error: {mse}')
        
        # Plot actual vs predicted values
        plt.figure(figsize=(10, 6))
        plt.scatter(y_test, y_pred, alpha=0.7)
        plt.plot([y_test.min(), y_test.max()], [y_test.min(), y_test.max()], 'k--', lw=2)
        plt.xlabel('Actual')
        plt.ylabel('Predicted')
        plt.title(f'Actual vs Predicted Values - {model_name}')
        plt.show()

    return results

# Train and evaluate models for home goals
logging.info("Evaluating models for home goals")
home_results = train_and_evaluate_models(X_home, y_home)

# Train and evaluate models for away goals
logging.info("Evaluating models for away goals")
away_results = train_and_evaluate_models(X_away, y_away)

logging.info(f"Home goals model results: {home_results}")
logging.info(f"Away goals model results: {away_results}")

# Select the best model
best_home_model = min(home_results, key=home_results.get)
best_away_model = min(away_results, key=away_results.get)

logging.info(f"Best model for home goals: {best_home_model}")
logging.info(f"Best model for away goals: {best_away_model}")

# Train and save the best model for home goals
logging.info("Training the best model for home goals")
best_home_model = LinearRegression()
best_home_model.fit(X_home, y_home)
joblib.dump(best_home_model, 'best_model_home_goals.pkl')

# Train and save the best model for away goals
logging.info("Training the best model for away goals")
best_away_model = SVR()
best_away_model.fit(X_away, y_away)
joblib.dump(best_away_model, 'best_model_away_goals.pkl')