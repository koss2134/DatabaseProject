--Names: Hamid Hemani, James Stewart, Mark Carvalho
-- Comp 4522 DBII Assingment 1
-- 3/FEB/2019


SET ECHO ON
SET FEEDBACK ON
SPOOL A1.log

SET SERVEROUT ON SIZE 10000

--Drop Tables and constraints on tables
DROP TABLE Article CASCADE CONSTRAINTS PURGE;
DROP TABLE Journal CASCADE CONSTRAINTS PURGE;
DROP TABLE Author CASCADE CONSTRAINTS PURGE; 
DROP TABLE ArticleAuthor CASCADE CONSTRAINTS PURGE; 
DROP TABLE Publisher CASCADE CONSTRAINTS PURGE;
DROP TABLE Keyword CASCADE CONSTRAINTS PURGE;
DROP TABLE ArticleKeywords CASCADE CONSTRAINTS PURGE;

--Auto sequence Drop and new creation for Journal table
DROP SEQUENCE Journal_ID_Seq;
CREATE SEQUENCE Journal_ID_Seq START WITH 1;

--Journal Table
CREATE TABLE Journal (
JOURNAL_ID INT NOT NULL UNIQUE,
SEC_TITLE_Publication VARCHAR2(1000),
PRIMARY KEY(JOURNAL_ID, SEC_TITLE_Publication)
);

--Trigger to check null entries before entering and add auto sequence for Journal Table
CREATE OR REPLACE TRIGGER Journal
   BEFORE INSERT ON Journal
   FOR EACH ROW 
   BEGIN
      IF :new.SEC_TITLE_Publication IS NULL THEN
         :new.SEC_TITLE_Publication := 'N/A';
      END IF;
      :new.JOURNAL_ID := Journal_ID_Seq.NEXTVAL;
   END;
/
SHOW ERRORS

--Publsher Table
CREATE TABLE Publisher (
PUB_ID INT NOT NULL UNIQUE,
PUBLISHER VARCHAR2(1000),
PRIMARY KEY(PUB_ID, PUBLISHER)
); 

--Auto Sequence Drop and new creation for Publisher table
DROP SEQUENCE Pub_ID_Seq;
CREATE SEQUENCE Pub_ID_Seq START WITH 1;

CREATE OR REPLACE TRIGGER Publisher
   BEFORE INSERT ON Publisher
   FOR EACH ROW 
   BEGIN
   IF :new.PUBLISHER IS NULL THEN
      :new.PUBLISHER := 'N/A';
   END IF;
      :new.PUB_ID := Pub_ID_Seq.NEXTVAL;
   END;
/
SHOW ERRORS

--Auto Sequence Drop and new creation
DROP SEQUENCE Article_ID_Seq;
CREATE SEQUENCE Article_ID_Seq START WITH 1;

--Article Table
CREATE TABLE Article (
ARTICLE_ID INT PRIMARY KEY NOT NULL,
OLD_DB_RECORD_NUM NUMBER(38),
TITLE VARCHAR2(1000),
JOURNAL_ID INT NOT NULL,
PUB_ID INT NOT NULL,
VOLUME VARCHAR2(1000),
DOI VARCHAR2(1000),
WORK_DATE VARCHAR2(1000),
YEAR NUMBER(38),
PAGES VARCHAR2(1000),
CONSTRAINT JOURNAL_ID_FK FOREIGN KEY (JOURNAL_ID) REFERENCES Journal(JOURNAL_ID),
CONSTRAINT PUB_ID_FK FOREIGN KEY (PUB_ID) REFERENCES Publisher(PUB_ID)
); 

--Trigger to add auto sequence before adding entries of articles
CREATE OR REPLACE TRIGGER Article
   BEFORE INSERT ON Article
   FOR EACH ROW 
   BEGIN
      :new.ARTICLE_ID := Article_ID_Seq.NEXTVAL;
   END;
/
SHOW ERRORS

--Auto Sequence Drop and new creation for Author Table
DROP SEQUENCE Author_ID_Seq;
CREATE SEQUENCE Author_ID_Seq START WITH 1;

CREATE TABLE Author (
AUTHOR_ID INT PRIMARY KEY not null,
AUTHOR VARCHAR2(4000)
); 

--Trigger to add auto sequence before entering for Author table
CREATE OR REPLACE TRIGGER Author
   BEFORE INSERT ON Author
   FOR EACH ROW 
   BEGIN
      :new.AUTHOR_ID := Author_ID_Seq.NEXTVAL;
   END;
/
SHOW ERRORS

--Auto Sequence Drop and new creation
DROP SEQUENCE AutKey_ID_Seq;
CREATE SEQUENCE AutKey_ID_Seq START WITH 1;

--ArticleAuthor Table
CREATE TABLE ArticleAuthor (
RECORDNUM INT PRIMARY KEY,
ARTICLE_ID INT NOT NULL,
AUTHOR_ID INT NOT NULL,
CONSTRAINT ARTICLE_ID_FK1 FOREIGN KEY (ARTICLE_ID) REFERENCES Article(ARTICLE_ID),
CONSTRAINT AUTHOR_ID_FK1 FOREIGN KEY (AUTHOR_ID) REFERENCES Author(AUTHOR_ID));

--Trigger to add auto sequence before entering for the relational ArticleAuthor table
CREATE OR REPLACE TRIGGER ArticleAuthor
   BEFORE INSERT ON ArticleAuthor
   FOR EACH ROW 
   BEGIN
      :new.RECORDNUM := AutKey_ID_Seq.NEXTVAL;
   END;
/
SHOW ERRORS

--Auto Sequence Drop and new creation
DROP SEQUENCE Key_ID_Seq;
CREATE SEQUENCE Key_ID_Seq START WITH 1;

--Keyword Table
CREATE TABLE Keyword (
KEY_ID INT PRIMARY KEY not null,
KEYWORDS  VARCHAR2(4000)
);

--Trigger to add auto sequence before entering for Keyword table
CREATE OR REPLACE TRIGGER Keyword
   BEFORE INSERT ON Keyword
   FOR EACH ROW 
   BEGIN
      :new.KEY_ID := Key_ID_Seq.NEXTVAL;
   END;
/
SHOW ERRORS

--Auto Sequence Drop and new creation
DROP SEQUENCE ArtKey_ID_Seq;
CREATE SEQUENCE ArtKey_ID_Seq START WITH 1;

--ArticleKeyword Table
CREATE TABLE ArticleKeywords (
RECORDNUM INT PRIMARY KEY,
KEY_ID INT NOT NULL,
ARTICLE_ID INT NOT NULL,
CONSTRAINT KEY_ID_FK FOREIGN KEY (KEY_ID) REFERENCES Keyword(KEY_ID),
CONSTRAINT ARTICLE_ID_FK2 FOREIGN KEY (ARTICLE_ID) REFERENCES Article(ARTICLE_ID)); 

--Trigger to ass auto sequence before entering for ArticleKeywords
CREATE OR REPLACE TRIGGER ArticleKeywords
   BEFORE INSERT ON ArticleKeywords
   FOR EACH ROW 
   BEGIN
      :new.RECORDNUM := ArtKey_ID_Seq.NEXTVAL;
   END;
/
SHOW ERRORS




-- I tried creating synonym for the AFEDORUK.SCIENCE1 Table but it gave me weird error saying no privilage.
--so you have to manully change the table on c1 cursor. <----!!!!!!!
-- CREATE SYNONYM SCIENCE FOR AFEDORUK.SCIENCE1;

------ DATA FILLING -----
DECLARE
record_no NUMBER(38);
title VARCHAR2(1000);
article_id INT;
j_id INT;
pub_id INT;
volume VARCHAR2(1000);
doi VARCHAR2(1000);
work_date VARCHAR2(1000);
year NUMBER(38);
apages VARCHAR2(1000);
author VARCHAR2(4000);
author_id INT;
num VARCHAR2(1000);
keywords VARCHAR2(4000);
stitle VARCHAR2(1000);
pub VARCHAR2(1000);
author_name_check VARCHAR2(4000);
title_check VARCHAR2(1000);
keyword_check VARCHAR2(1000);
key_id INT;

/*----------VARIABLE RELATED TO AUTHOR DELIMITING------------------------*/   
 names_adjusted VARCHAR2(4000);
 comma_location NUMBER := 0;
 prev_location NUMBER := 0;
 author_name VARCHAR2(4000);
 flag NUMBER := 0;
 /*----------VARIABLE RELATED TO KEYWORD DELIMITING------------------------*/ 
prev_location2 NUMBER := 0;
 comma_location2 NUMBER := 0;
 keywords_adjusted VARCHAR2(4000);
 key_word VARCHAR2(4000);
 flag2 NUMBER := 0;

 /*-------CURSORS-----------*/   

--Main cursor grabbing data from AFEDORUK.SCIENCE1 ~~ just replace AFEDORUK.SCIENCE2 or 3 here to change table.
--NOTE: COALESCE is handling the blank/empty fields and replacing it with N/A
CURSOR c1 IS SELECT RECORD_NO, TITLE, COALESCE(VOLUME, 'N/A'), COALESCE(DOI, 'N/A'), COALESCE(WORK_DATE, 'N/A'), YEAR, COALESCE(PAGES, 'N/A'), COALESCE(AUTHOR, 'N/A'), COALESCE(NUMBER_VOLUMES, 'N/A'), KEYWORDS, COALESCE(SEC_TITLE, 'N/A'), COALESCE(PUBLISHER, 'N/A') FROM AFEDORUK.SCIENCE1 WHERE LANGUAGE = 'eng';

--helper cursors
CURSOR c2 IS SELECT AUTHOR_ID, AUTHOR FROM Author;
CURSOR c3 IS SELECT ARTICLE_ID, TITLE FROM Article;
CURSOR c4 IS SELECT KEY_ID, KEYWORDS FROM Keyword;

BEGIN
   OPEN c1; 
      LOOP --main loop
      FETCH c1 INTO record_no, title, volume, doi, work_date, year, apages, author, num, keywords, stitle, pub;
		
      INSERT INTO Journal(SEC_TITLE_Publication) VALUES (stitle); --fill Journal Table
      INSERT INTO Publisher (PUBLISHER) VALUES (pub); --fill Publisher Table

      --journal_id and publisher_id selection
	  SELECT * INTO j_id FROM (SELECT JOURNAL_ID FROM Journal where SEC_TITLE_Publication = stitle) WHERE ROWNUM = 1;
      SELECT * INTO pub_id FROM (SELECT PUB_ID FROM Publisher where PUBLISHER = pub) WHERE ROWNUM = 1;
      
      --fill Article Table
      INSERT INTO Article (OLD_DB_RECORD_NUM, TITLE, JOURNAL_ID, PUB_ID, VOLUME, DOI, WORK_DATE, YEAR, PAGES) VALUES (record_no, title, j_id, pub_id, volume, doi, work_date, year, apages);

/*-------------AUTHOR TABLE FILLING WITH DELIMITERS----------------------------------*/      
   IF author IS NOT NULL THEN
      names_adjusted := author || ':';
      LOOP  -- loop within authors field 
         comma_location := INSTR(names_adjusted, ':', comma_location+1);
         EXIT WHEN comma_location = 0;
         author_name := SUBSTR(names_adjusted, prev_location+1, comma_location-prev_location-1);
         
          SELECT count(*)
            into flag
            from Author
            where AUTHOR = author_name; 
            
         IF  flag < 1 THEN
            IF author_name IS NOT NULL THEN
               INSERT INTO Author (AUTHOR) VALUES (author_name); -- fill Author Table
            END IF;
         END IF; 
         prev_location := comma_location;

            OPEN c3;
            LOOP --sub loop to open cursors on Article 
            FETCH c3 INTO article_id, title_check;
               IF title_check = title THEN --check if Title belongs to current article entry
                  OPEN c2;
                  LOOP --sub loop to open cursors on Author 
                  FETCH c2 INTO author_id, author_name_check;
                     IF author_name_check = author_name THEN --check if author is correct, then asigns that ID for entry into relation table.
                        INSERT INTO ArticleAuthor (ARTICLE_ID, AUTHOR_ID) VALUES (article_id, author_id); --fill ArticleAuthor Table
                     END IF;
                  EXIT WHEN c2%NOTFOUND;
                  END LOOP; --sub loop Author cursors ended
                  CLOSE c2;
               END IF;
            EXIT WHEN c3%NOTFOUND;
            END LOOP; -- sub loop of Article Cursor Ended
            CLOSE c3;

      END LOOP; -- Author delimiting loop ended
   END IF;
   
   /*-------------KEYWORDS TABLE FILLING WITH DELIMITERS----------------------------------*/ 
   IF keywords IS NOT NULL THEN
      keywords_adjusted := keywords || ':';

      LOOP -- loop within Keywords field 

         comma_location2 := INSTR(keywords_adjusted,':',comma_location2+1);
         EXIT WHEN comma_location2 = 0;
         key_word := SUBSTR(keywords_adjusted, prev_location2+1, comma_location2-prev_location2-1);
         
         SELECT count(*)
            into flag2
            from Keyword
            where KEYWORDS = key_word; 
            
         IF  flag2 < 1 THEN
            IF key_word IS NOT NULL THEN
               INSERT INTO Keyword(KEYWORDS) VALUES (key_word);
               /*DBMS_OUTPUT.PUT_LINE(key_word);*/
            END IF;
         END IF; 
         
         prev_location2 := comma_location2;

            OPEN c3;
            LOOP --sub loop to open cursor on Article 
            FETCH c3 INTO article_id, title_check;
               IF title_check = title THEN --check if Title belongs to Keywords
                  OPEN c4;
                  LOOP --sub loop to open cursor on Keyword
                  FETCH c4 INTO key_id, keyword_check;
                     IF keyword_check = key_word THEN --check if Keyword belongs to Title
                        INSERT INTO ArticleKeywords (KEY_ID, ARTICLE_ID) VALUES (key_id, article_id); --fill ArticleAuthor Table
                     END IF;
                  EXIT WHEN c4%NOTFOUND;
                  END LOOP; --sub loop Keyword cursors ended
                  CLOSE c4;
               END IF;
            EXIT WHEN c3%NOTFOUND;
            END LOOP; -- sub loop of Article Cursor Ended
            CLOSE c3;
      
      END LOOP; -- keywords delimiting loop ended
   END IF;


EXIT WHEN c1%NOTFOUND;
END LOOP; --end of main loop
CLOSE c1;
END;
/
SHOW ERRORS

