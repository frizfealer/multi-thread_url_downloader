# batch_url_downloader
A light-weight python class for downloading multiple URLs with multi-thread functions.

This class is for downloading a batch of URLs (currently with only HTTP GET).
It supports several features, including:
1. Multi-thread.
2. Logging, so if the downloading process is abrupted, it can resume back from the previous downloading state.

# Update
Using URLDownloader_v2 to download urls
Example usage:

```python
    sites = ["https://www.jython.org/",
             "http://olympus.realpython.org/dice",
             "https://mobileimages.lowes.com/product/converted/885612/885612278951.jpg"]
    out_name_list = ["1", "2", "3"]
    downloader = URLDownloader_v2(sites, 'test_out', 3, output_name_list=out_name_list) 
```
