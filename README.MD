*copinbase.py* collects OHLCV data from Coinbase and saves it to a PostgreSQL database.

*advancedfeatures.py* calculates advanced features from the **OHLCV** data and saves them to a **PostgreSQL** database.

**AA1_XGB.ipynb** is a Jupyter notebook that contains the code for the *best-performing *XGBoost* model* with a PARAM_GRID and Probability Threshold Tuning.
It has variables for customization and typically performs with an **F1 Score > .75**

There are also several notebooks that experiment with **Ensemble Stacking** (XGB, LGBM, SVM- Base Learners; XGB/Linear Regression META Learners)

SPY Performance Prediction Models are currently being tuned as data is collected.
