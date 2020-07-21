import abc
import os
import datetime
import fleep
from PIL import Image as PILImage
import shutil
import logging
import re

EXIF_DATE_TIME_ORIGINAL_TAG = 0x9003

OUT_DIR_FORMAT = "{0:0>4}-{1:0>2}"

logger = logging.getLogger("media_archiver")

# 1 minute delay
DIR_RUN_DELAY = 1 * 60


class RegexDateExtractor():

    def __init__(self, regex):
        self.re = re.compile(regex)
    
    def match(self, file_name):
        return self.re.match(file_name)

    def get_datetime(self, file_name):
        match = self.re.search(file_name)
        if match:
            logger.debug("{} -> year: {} month: {} day:{}".format(file_name, match.group('year'), match.group('month'), match.group('day')))
            return datetime.datetime(year=int(match.group('year')), month=int(match.group('month')), day=int(match.group('day')))
        return None

FILENAME_DATE_REGEX = [
    # images
    RegexDateExtractor(r"IMG-(?P<year>[0-9]{4})(?P<month>[0-9]{2})(?P<day>[0-9]{2})-WA[0-9]{4}.jpeg$"),         # IMG-20190605-WA0000.jpeg
    RegexDateExtractor(r"IMG-(?P<year>[0-9]{4})(?P<month>[0-9]{2})(?P<day>[0-9]{2})-WA[0-9]{4}.jpg$"),          # IMG-20190605-WA0000.jpg
    RegexDateExtractor(r"IMG_(?P<year>[0-9]{4})(?P<month>[0-9]{2})(?P<day>[0-9]{2})_[0-9]{6}.jpg$"),            # IMG_20200401_170719.jpg
    RegexDateExtractor(r"IMG_(?P<year>[0-9]{4})(?P<month>[0-9]{2})(?P<day>[0-9]{2})_[0-9]{6}~\d+.jpg$"),        # IMG_20200401_170719~2.jpg
    RegexDateExtractor(r"(?P<year>[0-9]{4})(?P<month>[0-9]{2})(?P<day>[0-9]{2})_[0-9]{6}.jpg$"),                # 20180701_173331.jpg
    RegexDateExtractor(r"(?P<year>[0-9]{4})(?P<month>[0-9]{2})(?P<day>[0-9]{2})_[0-9]{6}\(\d+\).jpg$"),         # 20180701_173331(0).jpg
    RegexDateExtractor(r"(?P<year>[0-9]{4})(?P<month>[0-9]{2})(?P<day>[0-9]{2})_[0-9]{6}_[0-9]{3}.jpg$"),       # 20180701_173331_001.jpg

    RegexDateExtractor(r"Screenshot_(?P<year>[0-9]{4})(?P<month>[0-9]{2})(?P<day>[0-9]{2})-[0-9]{6}.png"),      # Screenshot_20200307-132245.png

    # videos
    RegexDateExtractor(r"(?P<year>[0-9]{4})(?P<month>[0-9]{2})(?P<day>[0-9]{2})_[0-9]{6}.mp4$"),                # 20180701_173331.mp4
    RegexDateExtractor(r"(?P<year>[0-9]{4})(?P<month>[0-9]{2})(?P<day>[0-9]{2})_[0-9]{6}_\d+.mp4$"),            # 20180701_173331_1.mp4
    RegexDateExtractor(r"(?P<year>[0-9]{4})(?P<month>[0-9]{2})(?P<day>[0-9]{2})_[0-9]{6}\(\d+\).mp4$"),         # 20180701_173331(0).mp4
    RegexDateExtractor(r"VID-(?P<year>[0-9]{4})(?P<month>[0-9]{2})(?P<day>[0-9]{2})-WA[0-9]{4}.mp4$"),          # VID-20160428-WA0000.mp4
]

def datetime_from_filename(file_name):
    for extractor in FILENAME_DATE_REGEX:
        if extractor.match(file_name):
            return extractor.get_datetime(file_name)

    return None    


def move(src, dst, delete=False):
    logger.info("{} -> {}".format(src, dst))
    
    if delete:
        shutil.move(src, dst)
    else:
        shutil.copy2(src, dst)

class Handler(abc.ABC):

    def __init__(self, path, out_dir, delete):
        self.path = path
        self.out_dir = out_dir
        self.delete = delete
        self.file_name = os.path.basename(self.path)
        self.modified_time = datetime.datetime.fromtimestamp(os.path.getmtime(self.path))
        self.delay = 0

    @staticmethod
    @abc.abstractmethod
    def can_handle(path):
        raise NotImplementedError

    @abc.abstractmethod
    def run(self):
        raise NotImplementedError

    def _get_outdir(self, date_time):
        out = os.path.join(self.out_dir, OUT_DIR_FORMAT.format(date_time.year, date_time.month))
        
        if not os.path.exists(out) or not os.path.isdir(out):
            os.makedirs(out)
        
        return out
    
class Directory(Handler):

    def __init__(self, path, out_dir, delete):
        super().__init__(path, out_dir, delete)
        self.delay = DIR_RUN_DELAY 
    
    @staticmethod
    def can_handle(path):
        # dont process hidden files
        if os.path.basename(path).startswith("."):
            return False

        return os.path.isdir(path)

    def run(self):

        # dont process if we dont want to delete the dir
        if not self.delete:
            return True

        # we may have already been deleted at this point by a previous task.
        if not os.path.exists(self.path):
            return True

        def recursive_rm_dirs(path):
            if not os.path.exists(path):
                return 

            for root, dirs, _ in os.walk(path):

                # process directories, delete them bottom up
                for d in dirs:
                    curr_dir = os.path.join(root, d)
                    recursive_rm_dirs(curr_dir)

                # only delete it if its empty
                if len(os.listdir(root)) == 0:
                    logger.info("Deleting {}".format(root))
                    os.rmdir(root)                


        recursive_rm_dirs(self.path)

        # if it still exists, we have files that have not been processed yet, so reschedule
        if os.path.exists(self.path):
            return False
        
        return True

        

class Image(Handler):

    def __init__(self, path, out_dir, delete):
        super().__init__(path, out_dir, delete)
        
    @staticmethod
    def can_handle(path):
        if not os.path.isfile(path):
            return False

        # dont process hidden files
        if os.path.basename(path).startswith("."):
            return False

        with open(path, "rb") as file:
            info = fleep.get(file.read(128))
            if len(info.type) == 0:
                return False
          
        return info.type[0] in ["raster-image", "raw-image"]

    def _exif_datetime(self):
        try:
            exif = PILImage.open(self.path)._getexif()
        except Exception:
            return None

        if exif:
            dt = exif.get(EXIF_DATE_TIME_ORIGINAL_TAG, None)
            if dt and dt != '0000:00:00 00:00:00':
                try:
                    return datetime.datetime.strptime(dt, "%Y:%m:%d %H:%M:%S")
                except ValueError:
                    return datetime.datetime.strptime(dt, "%Y-%m-%d %H:%M:%S")
                    
        return None

    def get_date(self):
        """
        We can be more specific here and find the creation time embedded within the image. If that exists,
        use this in place of the modified time.
        """

        # if the exif data exists, use this over everything else
        exif_created_date = self._exif_datetime()
        if exif_created_date:
            return exif_created_date
        
        # if we failed to find exif data, use the filename
        file_name_datetime = datetime_from_filename(self.file_name)
        if file_name_datetime:
            return file_name_datetime

        # if we could not extract the date from the filename, use the modified time.
        return self.modified_time 

    def run(self):
            out = self._get_outdir(self.get_date())
            
            # dont process the file if it already exists, this includes duplicate names.
            out_path = os.path.join(out, self.file_name)
            if os.path.isfile(out_path):
                return True

            move(self.path, out_path, self.delete)
            return True


class Video(Handler):

    def __init__(self, path, out_dir, delete):
        super().__init__(path, out_dir, delete)

    @staticmethod
    def can_handle(path):
        if not os.path.isfile(path):
            return False

        # dont process hidden files
        if os.path.basename(path).startswith("."):
            return False

        with open(path, "rb") as file:
            info = fleep.get(file.read(128))
            if len(info.type) == 0:
                return False

        return info.type[0] in ["video"]

    def get_date(self):
        # use the filename
        file_name_datetime = datetime_from_filename(self.file_name)
        if file_name_datetime:
            return file_name_datetime

        # if we could not extract the date from the filename, use the modified time.
        return self.modified_time

    def run(self):
            out = self._get_outdir(self.get_date())
            
            # dont process the file if it already exists, this includes duplicate names.
            out_path = os.path.join(out, self.file_name)
            if os.path.isfile(out_path):
                return True

            move(self.path, out_path, self.delete)
            return True

class Audio(Handler):

    def __init__(self, path, out_dir, delete):
        super().__init__(path, out_dir, delete)

    @staticmethod
    def can_handle(path):
        if not os.path.isfile(path):
            return False
        
        # dont process hidden files
        if os.path.basename(path).startswith("."):
            return False

        with open(path, "rb") as file:
            info = fleep.get(file.read(128))
            if len(info.type) == 0:
                return False

        return info.type[0] in ["audio"]

    def get_date(self):
        # use the filename
        file_name_datetime = datetime_from_filename(self.file_name)
        if file_name_datetime:
            return file_name_datetime

        # if we could not extract the date from the filename, use the modified time.
        return self.modified_time

    def run(self):
        out = self._get_outdir(self.get_date())
            
        # dont process the file if it already exists, this includes duplicate names.
        out_path = os.path.join(out, self.file_name)
        if os.path.isfile(out_path):
            return True

        move(self.path, out_path, self.delete)
        return True


def handler_factory(f, out_dir, delete):
    
    handlers = [Image, Video, Audio, Directory]

    for handler in handlers:
        if handler.can_handle(f):
            return handler(f, out_dir, delete)
    return None