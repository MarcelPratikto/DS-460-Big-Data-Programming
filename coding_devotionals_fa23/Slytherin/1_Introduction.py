import streamlit as st
import pandas as pd
import numpy as np

st.set_page_config(
    page_title="Devotional Challenge",
    page_icon="🐍",
)

st.title("Devotional Challenge")


# Displaying team name in a more visually appealing manner
st.markdown("""
## Team Slytherin 🐍
""")

st.markdown('### Team Members and Majors')
st.markdown('---')


team_members = pd.DataFrame({
    'Name': ['Sulove Dahal', 'Spencer Howe','Brandon Lythgoe', 'David Peck', 'Kyle Baker', 'Aaron Jones' ],
    'Major': ['SWE / ML', 'Data Science', 'Data Science', 'Data Science', 'Data Science', 'Computer Science' ]
})

for index, row in team_members.iterrows():
    st.markdown(f"**{row['Name']}** - *{row['Major']}*")