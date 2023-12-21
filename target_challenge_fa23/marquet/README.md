Quick note on the church building data:
It's missing 2 church buildings in washington DC (cause the webpage is formatted different). I was gonna add them, but then forgot before I ran it through the address API. So here's those:
missed = [
    {'initial_address': '522 Seventh Street SE','initial_county': 'Washington, DC', 'state': 'va'},
    {'initial_address': '4901 16th Street Northwest','initial_county': 'Washington, DC', 'state': 'va'},
]
And 18 churches in Utah are missing. I didn't have the patience to scout through all the counties in Utah to find which ones, but there's over a 1,000 in Utah, so it's probably fine.
