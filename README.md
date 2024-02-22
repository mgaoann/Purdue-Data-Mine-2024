<div align="center">
  <a href="https://github.com/Wikidata/Purdue-Data-Mine-2024"><img src="https://raw.githubusercontent.com/Wikidata/Purdue-Data-Mine-2024/main/.github/Resources/WMDE_PDM_2024_GitHubBanner.png" width=1024 alt="Project Banner"></a>
</div>

# Wikimedia Deutschland x The Data Mine

This repository contains the program materials and student work for Wikimedia Deutschland's project in the [2024 Purdue Data Mine](https://datamine.purdue.edu/). Students will focus on comparing data from [Wikidata](https://www.wikidata.org/) with external data sources and then derive and report mismatches for the [Wikidata Mismatch Finder](https://www.wikidata.org/wiki/Wikidata:Mismatch_Finder). The corrections of these mismatches by the Wikidata community will then serve to improve Wikidata's data and all downstream projects including [Wikipedia](https://www.wikipedia.org/).

## **Contents**

- [MismatchGeneration](https://github.com/Wikidata/Purdue-Data-Mine-2024/tree/main/MismatchGeneration)
  - Student work to derive mismatches between Wikidata and external sources
- [Notebooks](https://github.com/Wikidata/Purdue-Data-Mine-2024/tree/main/Notebooks)
  - Program materials to introduce Python, Jupyter, Wikidata data access and more

## Process

Below is a flowchart describing the process used to generate mismatches.

```mermaid
flowchart TD
 subgraph Verify["Verify"]
    direction TB
        D("For example, if my chosen DB is MusicBrainz, 
        I would search for “P:Musicbrainz”. 
        I would then see that there are properties 
        like “MusicBrainz release group ID (P436)” 
        which links a WD album to a MB album.")
        C("You can search for an external ID 
        property by typing P: 
        in the Search on the Wikidata website")
        B("Verify that external identifiers 
        properties that link WD items 
        with that DB exist in WD")
  end
 subgraph Items["Items"]
    direction TB
        F("Run a query on query.wikidata.org 
        counting how many items have the 
        external ID property you found.")
        E("Verify that a lot of Wikidata items 
        are linked to that database (we want 
        our code to be useful for a lot of entries)")
  end
 subgraph 1["1"]
    direction LR
        Verify
        Items
        A("Choose a database that has 
        links to Wikidata to compare 
        information against")
  end
 subgraph research["research"]
    direction TB
        search("Search 'database name API' in Google.")
        resear("Research if it has an API endpoint 
            (a URL we can use to access data)")
        public("Make sure the API is publicly accessible (not paid).")
        unofficial("The API may be an “unofficial API” 
            where somebody has scrapped the data 
            or used an alternative method to get 
            data from a website. Verify this API 
            is up-to-date in information, still works, 
            and something we can query at scale.")
  end
 subgraph 2["2"]
    direction LR
        find("Find a way to access that database's
         data so we can compare it with Wikidata's")
        research
        ifapi("Research if there is a dump. 
        Dumps will allow us to download 
        the entire DB at once and not have
        make many REST requests")
        ifNoAPI("Look into using a Python library that can do web scraping.")
  end
 subgraph decide["decide"]
    direction TB
        mb["MusicBrainz stores artist data, 
            so I could decide to compare artist 
            data in Wikidata with artist data 
            in MusicBrainz."]
        decide1("Decide which data type you 
            would like to first compare 
            and find mismatches for")
  end
 subgraph attribute["attribute"]
    direction TB
        ex["MusicBrainz stores the founding 
            date of an artist and Wikidata 
            does as well using “debut date” (P10673)"]
        att("Decide which attribute of 
            your data type you would 
            like to compare")
  end
 subgraph 3["3"]
    direction LR
        decide
        attribute
  end
 subgraph test["test"]
    direction TB
        write["Write python code to get 
            that data using the “requests” 
            library for URLs or another method."]
        find2["Find the ID of an entity 
            in that database and then plug 
            it into a URL for the API 
            or query for it in a dump."]
        test1("Test out getting one item from 
            the DB first. Then you will 
            know how to get many.")
  end
 subgraph compare["compare"]
    direction TB
        comp1("The returned object from the API 
            might be in a Python dict or 
            list so use dict[“key”] to get to it.")
        comp("Write the code to get the 
            data from the decided attribute 
            you would like to compare with Wikidata")
  end
 subgraph 4["4"]
    direction LR
        method("Get the data from the 
        database using the method 
        you chose above")
        test
        compare
  end
 subgraph qid["qid"]
    direction TB
        mb2["With the MusicBrainz example, 
            we would be filtering for items 
            that have “MusicBrainz artist ID (P434)” 
            and “debut date” (P10673)”"]
        dir["In other words, find which items have the:
1. External identifier property you researched 
earlier that link the Wikidata item to the database. 
2. The attribute property you decided on

You can excute a SPARQL query on the 
Wikidata Query Service or QLever to do this."]
        list("Get the list of item IDs (QIDs) 
            from Wikidata that are linked to 
            the database and have the property 
            for the attribute we are looking 
            to generate mismatches for")
  end
 subgraph 5["5"]
    direction LR
        get("Get the Wikidata items that 
        are linked to the database 
        and their data to compare 
        with the database’s data")
        qid
        filter("Filter the Wikidata dump 
        for these items with these 
        IDs using Python")
  end
 subgraph loop["loop"]
    direction TB
        iterate2("In each iteration:
            1. Get the ID of entity in the external database from the WD item
            2. Use the ID and your code from before to make a query to 
            the API or dump to get the entity's data in that database
            3. Use your code from before to get the attribute from that 
            database and compare it with the value in Wikidata
            4. If they the values are not equivalent add the relevant 
            information about the mismatch to a mismatch dataframe of <a href="https://github.com/wmde/wikidata-mismatch-finder/blob/main/docs/UserGuide.md#creating-a-mismatches-import-file">this format</a>")
        iterate("Write Python code to iterate 
            through each of the Wikidata items 
            you filtered and find if the Wikidata 
            item attribute matches with 
            that from the other database")
  end
 subgraph 6["6"]
    direction LR
        compare2("Get and compare the data 
        in the external database 
        with that from Wikidata")
        loop
        mis("Use the Mismatch Finder validator from 
        utils.py to validate that your mismatch 
        dataframe is correct.")
  end
 subgraph 7["7"]
    direction LR
        convert("Convert the the mismatch dataframe to a 
        CSV. Make sure to export it without an index 
        column in pandas. and <a href="https://github.com/wmde/wikidata-mismatch-finder/blob/main/docs/UserGuide.md#getting-upload-rights">upload it to the Mismatch Finder</a>")
        upload("Upload the mismatches")
  end
    B --> C
    C --> D
    E --> F
    A --> Verify
    Verify ---> Items
    resear --> search
    search --> public & unofficial
    find --> research
    research -- if API --> ifapi
    research -- if no API --> ifNoAPI
    decide1 --> mb
    att --> ex
    decide --> attribute
    test1 --> find2
    find2 --> write
    comp --> comp1
    method --> test
    test --> compare
    list --> dir
    dir --> mb2
    get --> qid
    qid --> filter
    iterate --> iterate2
    compare2 --> loop
    loop --> mis
    upload --> convert
    1 --> 2
    2 --> 3
    3 --> 4
    4 --> 5
    5 --> 6
    6 --> 7
```