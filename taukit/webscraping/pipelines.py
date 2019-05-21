"""Scrapy pipeline classes."""
# pylint: disable=unused-argument


class TauItemPipeline:
    """Item pipeline for storing spiders' output."""

    def __init__(self):
        """Initialization method."""
        self.db = None
        self.disk = None

    def open_spider(self, spider):
        if spider.storage.lower() not in ('no', 'none'):
            self.setup_disk_storage(spider)
        if spider.storage.lower() not in ('no', 'none', 'nodb'):
            self.setup_db_storage(spider)

    def setup_disk_storage(self, spider):
        raise NotImplementedError

    def setup_db_storage(self, spider):
        raise NotImplementedError

    def process_item(self, item, spider):
        # pylint: disable=unused-argument
        if self.disk:
            self.disk.save(item)
        return item

    def close_spider(self, spider):
        if self.db:
            items = self.disk.load()
            self.db.bulk_update(items)
