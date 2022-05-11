import re

class BaseRegExPreprocessing:
    regex = r""
    replace_token = " <GENERIC_TOKEN> "

    def preprocess(self, text):
        return re.sub( self.regex, self.replace_token, text )

class RegExRemovePunkt(BaseRegExPreprocessing):
    regex = r"\W"
    replace_token = ""

class RegExReplaceNumberLike(BaseRegExPreprocessing):
    regex = r"(?:\d+.)+"
    replace_token = " <NUMBER_LIKE> "

class RegExReplaceMoney(BaseRegExPreprocessing):
    regex = r'\w[$][ ]\d{1,3}(?:\.\d{3})*?,\d{2}'
    replace_token = " <MONEY> "

class RegExReplacePhone(BaseRegExPreprocessing):
    #+55 84 99912-8191
    regex = r"(?:\+[0-9]+)*\s*[0-9]{0,5}\s*[0-9]{3,5}\-[0-9]{3,5}"
    replace_token = " <PHONE_NUMBER> "

class RegExReplaceEMail(BaseRegExPreprocessing):
    regex = r"\w+\@\w+(?:\.\w+)+"
    replace_token = " <EMAIL> "

class Lowercase():
    def preprocess(self, text):
        return text.lower()