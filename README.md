# grid_space_display
Mirror on gitlab: https://gitlab.cern.ch/turra/grid_space_display

Tool to visualize the space used by each user of a grid space token, as localgroupdisk as a function of time, in a web page. Live example: https://turra.web.cern.ch/localgroupdiskusage/


## Local test

To test the web page locally, run:

```
cd interface
python -m http.server 8080
```

Then go to `http://localhost:8080/` in your browser.
