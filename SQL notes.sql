# ADD a column after a specified column in a table:
ALTER TABLE tbl_link_09 ADD COLUMN url varchar(255) AFTER xlink_href;

# Update a column based on the values in another column of the table:
UPDATE tbl_link_09 LK JOIN tbl_article_wiki13 AR ON LK.xlink_href=AR.id SET LK.url = CONCAT('www.wikipedia.com/', REPLACE(AR.title,' ', '_'));

# Show index of a table:
show index from tbl_link_09_new;

