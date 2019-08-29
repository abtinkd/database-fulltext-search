# ADD a column after a specified column in a table:
ALTER TABLE tbl_link_09 ADD COLUMN url varchar(255) AFTER xlink_href;

# Update a column based on the values in another column of the table:
UPDATE tbl_link_09 LK JOIN tbl_article_wiki13 AR ON LK.xlink_href=AR.id SET LK.url = CONCAT('www.wikipedia.com/', REPLACE(AR.title,' ', '_'));

# Show index of a table:
show index from tbl_link_09_new;

# Updare a column if:
UPDATE tbl_link_09 LK SET url=
CASE 
	WHEN LK.xlink_href<>LK.url THEN REPLACE(LK.url, '_', ' ') 
	ELSE LK.url 
END;

# REGEX_REPLACE  function:
# reference: https://techras.wordpress.com/2011/06/02/regex-replace-for-mysql/
DELIMITER $$
CREATE FUNCTION  `regex_replace`(pattern VARCHAR(1000),replacement VARCHAR(1000),original VARCHAR(1000))

RETURNS VARCHAR(1000)
DETERMINISTIC
BEGIN 
 DECLARE temp VARCHAR(1000); 
 DECLARE ch VARCHAR(1); 
 DECLARE i INT;
 SET i = 1;
 SET temp = '';
 IF original REGEXP pattern THEN 
  loop_label: LOOP 
   IF i>CHAR_LENGTH(original) THEN
    LEAVE loop_label;  
   END IF;
   SET ch = SUBSTRING(original,i,1);
   IF NOT ch REGEXP pattern THEN
    SET temp = CONCAT(temp,ch);
   ELSE
    SET temp = CONCAT(temp,replacement);
   END IF;
   SET i=i+1;
  END LOOP;
 ELSE
  SET temp = original;
 END IF;
 RETURN temp;
END$$
DELIMITER ;

