import json

class JsonWriterPipeline:
    def open_spider(self, spider):
        self.file = open('items.json', 'w')
        self.file.write('[')
        self.first_item = True

    def close_spider(self, spider):
        self.file.write(']')
        self.file.close()

    def process_item(self, item, spider):
        if not self.first_item:
            self.file.write(',\n')
        else:
            self.first_item = False

        line = json.dumps(dict(item), indent=4)
        self.file.write(line)
        return item