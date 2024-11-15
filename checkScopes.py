# Quick check of token scopes
import pickle

with open('token.pickle', 'rb') as token:
    creds = pickle.load(token)
    print("Current scopes:", creds.scopes)