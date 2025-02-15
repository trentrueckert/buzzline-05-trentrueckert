import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
import time

# Connect to SQLite database
db_path = "C:/Users/trent/Projects/buzzline-05-trentrueckert/data/buzz.sqlite"
conn = sqlite3.connect(db_path)

# Create a figure and axes for plotting (only 3 plots, not 4)
fig, axs = plt.subplots(2, 2, figsize=(10, 8))

# Hide the 4th plot (axs[1, 1]) since you don't need it
axs[1, 1].axis('off')  # This removes the fourth axis

# Function to update the plots
def update_plots():
    # Fetch updated data
    query = """
    SELECT message, category, sentiment
    FROM streamed_messages;
    """
    df = pd.read_sql_query(query, conn)

    # 1. Category Counts
    category_counts = df['category'].value_counts()
    axs[0, 0].cla()  # Clear previous plot
    axs[0, 0].bar(category_counts.index, category_counts.values, color='skyblue', edgecolor='black')
    axs[0, 0].set_title('Total Category Counts')
    axs[0, 0].set_ylabel('Count')
    axs[0, 0].tick_params(axis='x', rotation=45)

    # 2. Sentiment Distribution
    axs[0, 1].cla()  # Clear previous plot
    df['sentiment'].plot(kind='hist', bins=10, ax=axs[0, 1], color='lightgreen', edgecolor='black')
    axs[0, 1].set_title('Sentiment Score Distribution')
    axs[0, 1].set_xlabel('Sentiment Score')
    axs[0, 1].set_ylabel('Frequency')

    # 3. Sentiment by Category
    category_sentiment = df.groupby('category')['sentiment'].mean()
    axs[1, 0].cla()  # Clear previous plot
    axs[1, 0].bar(category_sentiment.index, category_sentiment.values, color='orange', edgecolor='black')
    axs[1, 0].set_title('Average Sentiment by Category')
    axs[1, 0].set_xlabel('Category')
    axs[1, 0].set_ylabel('Average Sentiment')
    axs[1, 0].tick_params(axis='x', rotation=45)

    # Refresh the plot
    plt.tight_layout()  # Adjust layout to avoid overlap

# Main loop for updating the plots
plt.ion()  # Turn on interactive mode to continuously update
while True:
    update_plots()  # Update the plots with new data
    plt.draw()  # Redraw the figure with the updated plots
    plt.pause(5)  # Pause for 5 seconds before updating again

    # Exit after a certain number of loops for testing purposes
    # Remove or comment out the following line to make the loop run indefinitely:
    # break