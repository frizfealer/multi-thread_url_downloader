import concurrent.futures
import requests
import threading
import time
import io
# from PIL import Image
from urllib.parse import urljoin, urlparse
import mimetypes
import os
#from requests.adapters import HTTPAdapter
import sys
import hashlib
import collections
import random

thread_local = threading.local()

def remove_query_from_url(url):
    """ 
    This function remove the query term in the url.
    
    Parameters: 
        url (string)

    Returns: 
        the processed url (string)
    """
    return urljoin(url, urlparse(url).path) 

def is_url_image(url):  
    """ 
    This function detects if the given url is an image.
    
    Parameters: 
        url (string)

    Returns: 
        whether it is an image url or not(boolean)
    """
    url = remove_query_from_url(url)  
    mimetype, _ = mimetypes.guess_type(url)
    return (mimetype and mimetype.startswith('image'))

def get_session():
    """ 
    This function generates one session for each thread:
    
    Parameters: 
        None

    Returns: 
        None
    """
    if not hasattr(thread_local, 'session'):
        thread_local.session = requests.Session()
    return thread_local.session

class URLDownloader:
    """ 
    This is a class for downloading a batch of urls via http connection.
      
    Attributes: 
        url_list (list): a list of url to download.
        out_path (string): the path to the output folder /
        outpath_list (list): appending the outname with outpath.
        num_thread (int): the number of thread used for download.
        err_tolerance_num (int): the number of error tolerance for downloading.
        stop_interval (int): the secs to stop after error number exceeds the  err_tolerance_num
        time_out_for_GET (int): the time limit for http GET
        http_headers (dict): the header for http.
        outname_list (list): the list for the output file name. The default behaviour is using the file name in the url. If this is specified, it will overwrite the default name.
        err_cnter (int): counter for counting consecutive errors.
        log_file (string): a file name for logging, saving inside the out_path.
        _errs_cnter_lock (RLock): avoid race condition. this lock is for err_cnter
        __log_lock (Lock): avoid race condition. this lock is for log_file
    """
    def __init__(self, url_list,
                 out_path,
                 num_thread=4,
                 err_tolerance_num=1000,
                 stop_interval=0,
                 time_out_for_GET=600,
                 http_headers={},
                 remove_dup_img=False, 
                 outname_list=None,
                 ):
        """ 
        The constructor for URLDownloader Class. It saves the parameters as attributes, set some attributes, and call update_downloading_status
        
        Parameters: 
            url_list (list): a list of url to download.
            out_path (string): the path to the output folder 
            num_thread (int): the number of thread used for download.
            err_tolerance_num (int): the number of error tolerance for downloading.
            stop_interval (int): the secs to stop after error number exceeds the  err_tolerance_num
            time_out_for_GET (int): the time limit for http GET
            http_headers (dict): the header for http.
            remove_dup_img (boolean): whether to remove the same image with different urls.
            outname_list (list): the list for the output file name. The default behaviour is using the file name in the url. If this is specified, it will overwrite the default name.
                    
        Returns: 
            The URLDownloader object
        """
        self.out_path = out_path
        if outname_list:
            assert(len(url_list) == len(outname_list))
            url2oname = {url: oname for url, oname in zip(url_list, outname_list)}
            url_list = [k for k in url2oname.keys()]
            outname_list = [v for v in url2oname.values()]
        else:
            url_list = list(set(url_list))
        if outname_list:
            self.outpath_list = [os.path.join(out_path, name) for name in outname_list]
        else:
            self.outpath_list = [self.get_outpath_from_url(i) for i in url_list]
        self.url_list = url_list
        self.num_thread = num_thread
        self.err_tolerance_num = err_tolerance_num
        self.stop_interval = stop_interval
        self.time_out_for_GET = time_out_for_GET
        self.http_headers = http_headers

        self.err_cnter = 0
        self.url_cnter = 0
        self.log_file = os.path.join(out_path, 'downloaded.log')
        self._errs_cnter_lock = threading.Lock()
        self._log_lock = threading.Lock()
        # self._check_url_lock = threading.Lock()
        if not os.path.exists(out_path): 
            print('output folder is not exist, create "{}" folder'.format(out_path))
            os.makedirs(out_path)
        if not os.path.exists(self.log_file): 
            f = open(self.log_file, 'w')
            f.close()
        #self.adapter = HTTPAdapter(max_retries=3)
        #self.check_urls()
        self.update_downloading_status()

    def update_downloading_status(self):
        """ 
        The function to update the url_list, outpaht_list, and img_hash_set, based on the log in the folder.
        This is useful when you already have some urls downloaded in the output folder.
        This function depnds on the log file in the output folder.

        Parameters: 
            None  

        Returns: 
            None
        """
        tmp = []
        with open(self.log_file, 'r') as f:
            downloaded_url = collections.Counter([line.split('\t')[0] for line in f])
        for url, outpath in zip(self.url_list, self.outpath_list):
            if url in downloaded_url:
                downloaded_url[url] -= 1
                if downloaded_url[url] == 0:
                    downloaded_url.pop(url)
            else:
                tmp.append((url, outpath))
        # random.shuffle(tmp)
        self.url_list = [i[0] for i in tmp]
        self.outpath_list = [i[1] for i in tmp]

    def get_num_urls_needed(self):
        """ 
        This function returns the number of urls needs to download to the output folder.
        
        Parameters: 
            None  

        Returns: 
            len(self.url_list) (int)
        """
        self.update_downloading_status()
        return len(self.url_list)

    def get_outpath_from_url(self, url):
        """ 
        This function generate output path for an url, based on the filename in an url.
        
        Parameters: 
            url (string)  

        Returns: 
            outpath (string): output path for the given url
        """
        parsing = urlparse(url)
        if os.path.basename(parsing.path):
            fname = os.path.basename(parsing.path)
        else:
            fname = parsing.netloc
        outpath = os.path.join(self.out_path, fname)
        return outpath

    def download_site(self, url, outpath):
        """ 
        This function download an url and save its content to the outpath.
        
        Parameters: 
            url (string): the url to downalod.
            outpath (string): the output path to save the content.

        Returns: 
            None
        """
        session = get_session()
        session.headers.update(self.http_headers)
        #session.mount(url, self.adapter) #for retry
        #print('url: {}, outpath: {}'.format(url, outpath))
        with session.get(url, timeout=self.time_out_for_GET) as response:
            if response:
                print('o', end='', file=sys.stderr, flush=True)
                self.url_cnter += 1
                if self.url_cnter % 1000 == 0:
                    print('# processed url: {}...'.format(self.url_cnter), end='', file=sys.stderr, flush=True)
                #print(f"Read {len(response.content)} from {url}")
                with open(outpath, 'wb') as f:
                    f.write(response.content)
                with self._log_lock:
                    with open(self.log_file, 'a') as f:
                        f.write('{}\t{}\n'.format(url, 'o'))
                with self._errs_cnter_lock:
                    self.err_cnter = 0
            else:
                print('x', end='', file=sys.stderr, flush=True)
                self.url_cnter += 1
                if self.url_cnter % 1000 == 0:
                    print('# processed url: {}...'.format(self.url_cnter), end='', file=sys.stderr, flush=True)
                with self._errs_cnter_lock:
                    if self.err_cnter >= self.err_tolerance_num:
                        time.sleep(self.stop_interval)
                        self.err_cnter = 0
                        print('last error code is {}, error url: {}'.format(response.status_code, url), file=sys.stderr, flush=True)
                    else:
                        self.err_cnter += 1
                with self._log_lock:
                    with open(self.log_file, 'a') as f:
                        f.write('{}\t{}\n'.format(url, 'x'))

    def download_all_sites(self):
        """ 
        This function calls [self.num_thread] threads to download urls.
        Its a multithread version of download_site
        
        Parameters: 
            None
        Returns: 
            None
        """
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.num_thread) as executor:
                executor.map(self.download_site, self.url_list, self.outpath_list)

if __name__ == "__main__":
    import shutil
    if os.path.exists('test_out'):
        shutil.rmtree('test_out')
    sites = [
        "https://www.jython.org/",
        "http://olympus.realpython.org/dice",
        'https://mobileimages.lowes.com/product/converted/885612/885612278951.jpg?size=xl',
        'https://images.lowes.com/product/converted/885612/885612277671lg.jpg',
        'https://images.lowes.com/product/converted/885612/885612279095lg.jpg'
    ] 
    print('testing constructor...')
    downloader = URLDownloader(sites, 'test_out', 3, outname_list=['1', '2', '3', '4', '5'])
    print('the urls need to be downloaded:')
    print(downloader.url_list)
    print('the output path to save files:')
    print(downloader.outpath_list)
    print('--------------------------------------------------------------------')
    input('Press "Enter" to continue...')

    print('testing download_site...')
    downloader.download_site(downloader.url_list[0], downloader.outpath_list[0])
    downloader.download_site(downloader.url_list[1], downloader.outpath_list[1])
    print('update_downloading_status...')
    downloader.update_downloading_status()
    print('the urls need to be downloaded:')
    print(downloader.url_list)
    print('the output path to save files:')
    print(downloader.outpath_list)    
    print('--------------------------------------------------------------------')
    input('Press "Enter" to continue...')

    print('testing download_all_sites..')
    downloader.download_all_sites()
    print('update_downloading_status...')
    downloader.update_downloading_status()
    print('the urls need to be downloaded:')
    print(downloader.url_list)
    print('the output path to save files:')
    print(downloader.outpath_list)
    print('--------------------------------------------------------------------')
    input('Press "Enter" to continue...')

    print('testing error urls...')
    sites = [s + 'abcde' for s in sites]
    print(sites)
    if os.path.exists('test_out2'):
        shutil.rmtree('test_out2')
    downloader = URLDownloader(sites, 'test_out2', 3, err_tolerance_num=10, stop_interval=5)
    downloader.download_all_sites()
    print('--------------------------------------------------------------------')
    input('Press "Enter" to continue...')
