from bs4 import BeautifulSoup
import requests
import numpy as np
from datetime import date
import pickle
import os
import glob
import pandas as pd
#import smtplib, ssl
#from email import encoders
#from email.mime.base import MIMEBase
#from email.mime.multipart import MIMEMultipart
#from email.mime.text import MIMEText
import yagmail

timestamp = str(date.today()).replace("-","_")
path = os.getcwd()

def get_text(url= "https://visionfund.com/team"):

    """

    INPUT: url = specifically, Vision Fund's team page
    OUTPUT: returns a string list in the shape of [["name", "title#1, title#2, region"], [etc]

    """
    r = requests.get(url)
    html_contents = r.text
    html_soup = BeautifulSoup(html_contents, features="html.parser")
    team = html_soup.find_all(class_='team-card__text')
    split_text = [entry.text.splitlines()[1:] for entry in team]
    return split_text

def dict_from_text(text, verbose=False):

    """

    INPUT: text = a list two strings in the shape of [["name", "title#1, title#2, region"], [etc]]
    OUTPUT: return a dictionary with keys for names, titles and regions

    """
    regions = ['americas', 'asia', 'emea', "global"]

    # custom print function for when the verbose flag is True
    verboseprint = print if verbose else lambda *a, **k: None

    vf_team = {"names":[], "titles":[], "regions":[]}

    for entry in text:

        # names always come first
        vf_team["names"].append(entry[0])
        verboseprint("Looking at {}".format(entry[0]))

        # split the remain items by comma
        split_items = entry[1].split(',')
        verboseprint("Split items: {}".format(split_items))

        # each person gets a nested lists for titles and regions
        inner_title_list = []
        inner_region_list = []


        for item in split_items:
            # convert to lower case just in case
            item = item.strip().lower()
            verboseprint("..Item: {}".format(item))

            # drop spaces and see if there is a region match
            if item.replace(" ", "") in regions:
                verboseprint("....! regions hit: {}".format(item))
                inner_region_list.append(item.replace(" ", ""))

            else:
                # assume anything that's not a region is a title
                verboseprint("....! title hit in else loop: {}".format(item))
                inner_title_list.append(item)

                #since some titles have region wording in them, check each word
                verboseprint("........Checking each word")
                # spint string by space
                for word in item.split(" "):
                    # get rid of extra spaces
                    word = word.strip()
                    verboseprint("..........Word: {}".format(word))
                    if word.lower() in regions:
                        verboseprint("............! region hit in word subloop: {}".format(word))
                        inner_region_list.append(word.lower())



        # check that the list isn't empty before adding
        if inner_title_list:
            vf_team["titles"].append(inner_title_list)

        # check that the list isn't empty before adding
        if inner_region_list:
            vf_team["regions"].append(inner_region_list)
        # assignd None to bios without region
        else:
            vf_team["regions"].append(["none"])

        verboseprint("\n")

    return vf_team


split_text = get_text()
vf_team = dict_from_text(split_text)
with open(path + "/text_dicts/{}".format(timestamp), 'wb') as handle:
    pickle.dump(vf_team, handle, protocol=pickle.HIGHEST_PROTOCOL)

raw_df = pd.DataFrame.from_dict(vf_team)
raw_df.to_pickle(path + "/scrape_df/{}".format(timestamp))


def find_old (timestamp, path):

    """

    INPUT: timestamp for the current date, path to pwd
    OUTPUT: returns previous DataFrame and most recent for comparison

    """

    dates = [file[-10:] for file in glob.glob(path + "/scrape_df/20*")]
    old = pd.read_pickle(path + "/scrape_df/{}".format(dates[-2]))
    new = pd.read_pickle(path + "/scrape_df/{}".format(dates[-1]))

    return (old, new)

def find_departures_additions (old_df, new_df, new_date):

    """

    INPUT: current and previous DataFrames, assumes they contain some changes
    OUTPUT: stacked df of personelle changes

    """

    old_names = old_df["names"].tolist()
    new_names = new_df["names"].tolist()

    additions = list(set(new_names) - set(old_names))
    departures = list(set(old_names) - set(new_names))

    quit_df = old_df[old_df["names"].isin(departures)] if departures else pd.DataFrame()
    joined_df = new_df[new_df["names"].isin(additions)] if additions else pd.DataFrame()

    quit_df.set_index([["quit"]*len(quit_df)], inplace=True)
    joined_df.set_index([["joined"]*len(joined_df)], inplace=True)

    quit_joined_stack = quit_df.append([joined_df])

    quit_joined_stack.index.name = "action"
    quit_joined_stack["date"] = [new_date]*len(quit_joined_stack)
    quit_joined_stack.reset_index(inplace=True)
    quit_joined_stack.set_index(["date", "action"], inplace=True)

    return quit_joined_stack

def find_titles_regions (old_df, new_df, new_date):

    """

    INPUT: current and previous DataFrames, assumes they contain some changes
    OUTPUT: stacked df of title/region changes

    """

    old_names = old_df["names"].tolist()
    new_names = new_df["names"].tolist()

    additions = list(set(new_names) - set(old_names))
    departures = list(set(old_names) - set(new_names))

    old_cleaned = old_df[~old["names"].isin(departures)] if departures else old_df
    new_cleaned = new_df[~new["names"].isin(additions)] if additions else new_df

    old_cleaned = old_cleaned.rename(columns = {"titles":"old_titles", "regions":"old_regions"})
    new_cleaned = new_cleaned.rename(columns = {"titles":"new_titles", "regions":"new_regions"})

    merged = pd.merge(old_cleaned,new_cleaned, on="names")

    region_report_index = [i for i, row in merged.iterrows() if row["old_regions"]!=row["new_regions"]]
    title_report_index = [i for i, row in merged.iterrows() if row["old_titles"]!=row["new_titles"]]

    titles_change_df = merged[merged.index.isin(title_report_index)].drop(columns=["old_regions", "new_regions"])
    titles_change_df.set_index([["title_change"]*len(title_report_index)], inplace=True)
    titles_change_df.rename(columns = {"old_titles":"old", "new_titles":"new"}, inplace=True)

    region_change_df = merged[merged.index.isin(region_report_index)].drop(columns=["old_titles", "new_titles"])
    region_change_df.set_index([["region_change"]*len(region_report_index)], inplace=True)
    region_change_df.rename(columns = {"old_regions":"old", "new_regions":"new"}, inplace=True)

    titles_regions_stack = titles_change_df.append(region_change_df)

    titles_regions_stack.index.name = "action"
    titles_regions_stack["date"] = [new_date]*len(titles_regions_stack)
    titles_regions_stack.reset_index(inplace=True)
    titles_regions_stack.set_index(["date", "action"], inplace=True)

    return titles_regions_stack

def create_report (old_df, new_df, new_date, path):

    """

    INPUT: current and previous DataFrames and most recent date, pwd path
    OUTPUT: 4 html change reports -- personelle, title/region and respecitve logs

    """

    report_list = []
    message = ""

    quit_joined_stack = find_departures_additions(old_df, new_df, new_date)
    titles_regions_stack = find_titles_regions(old_df, new_df, new_date)

    if not quit_joined_stack.empty:

        previous_personelle_log = pd.read_pickle(path + "/change_logs/personelle_log")
        updated_personelle_log = previous_personelle_log.append(quit_joined_stack)
        updated_personelle_log.to_pickle(path + "/change_logs/personelle_log")

        html_personelle = quit_joined_stack.to_html()
        html_updated_personelle_log = updated_personelle_log.to_html()
        file_name = path + "/html_reports/personelle_changes_{}.html".format(new_date)
        html_file = open(file_name, "w")
        html_file.write(html_updated_personelle_log)
        html_file.close()

        report_list.append(file_name, html_updated_personelle_log)
        message += "Found changes in personelle.\n"

    else:
        message += "Found no changes in personelle.\n"


    if not titles_regions_stack.empty:

        previous_details_log = pd.read_pickle(path + "/change_logs/details_log")
        updated_details_log = previous_details_log.append(titles_regions_stack)
        updated_details_log.to_pickle(path + "/change_logs/details_log")

        html_details = titles_regions_stack.to_html()
        html_updated_details_log = updated_details_log.to_html()
        file_name = path + "/html_reports/detail_changes_{}.html".format(new_date)
        text_file = open(file_name, "w")
        text_file.write(html_updated_details_log)
        text_file.close()

        report_list.append(file_name, html_updated_details_log)
        message += "Found changes in title details.\n"

    else:
        message += "Found no changes in title details.\n"


    return (message, report_list)

old_df, new_df= find_old(timestamp, path)
message, report_list = create_report(old_df, new_df, timestamp, path)
attachment_list = [item for item in report_list if item]

receiver = "pashakun+vf@gmail.com"

yag = yagmail.SMTP("pashakun@gmail.com", oauth2_file="~/secret.json")
yag.send(
    to=receiver,
    subject="Your Vision Fund scrape results are here.",
    contents=message,
    attachments=attachment_list
)

file_name = path + "/run_logs/log_{}.html".format(timestamp)
text_file = open(file_name, "w")
text_file.write(message)
text_file.close()
