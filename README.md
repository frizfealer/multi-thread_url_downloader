# batch_url_downloader
A light-weight python class for downloading multiple URLs with thread.

This class is for downloading a batch of URLs (currently with only HTTP GET).
It supports several features, including:
1. Multi-thread.
2. Logging, so if the downloading process is abrupted, it can resume back from the previous downloading state.
3. duplicate image checking with md5. If the URL contains duplicate images, it uses md5 checksum to check the redundancy.


