import re


class RegexApplier():
    def __init__(self, instances=[]):
        self.instances = instances

    def replace(self, text):
        processed_text = text
        for instance in self.instances:
            pattern = instance['pattern']
            replace_token = instance['replace_token']
            processed_text = re.sub(pattern, replace_token, processed_text)

        return processed_text
