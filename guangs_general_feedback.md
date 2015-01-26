Guangs Notes for Code Review 1/25/2015
======================================
Specific comments are made directly as in-line comments, check commit history for details.
Here are some comments about overall stuff:

## .gitignore
- Make a .gitignore and add stuff to it that you dont want included in the repo, ie:
  - sensitive information like secret keys or password used to run shell scripts
  - trash/temporary files that are generated as back-up or auto-saves
  - data dumps that should only live on your local repo (other people dont need it to run stuff)
- Make sure you add this .gitignore to your repo so when people use your codes can see what
  not to put in repo etc

## website repo
- I would recommend making the web repo (your flask-app) a separate repo #modular :trollface:

## readme
- You should make a readme to have basic info as what the repo is about, how to run it, list
  some dependencies and brief documentation.


