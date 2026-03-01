INSERT INTO concept VALUES
    (1, 'Domain', 'Domain', 'Domain', 'Domain', NULL, 'Metadata', '01-Jan-1970', '31-Dec-2099', NULL),
    (2, 'Gender', 'Domain', 'Domain', 'Domain', NULL, 'OMOP generated', '01-Jan-1970', '31-Dec-2099', NULL),
    (3, 'Race', 'Domain', 'Domain', 'Domain', NULL, 'OMOP generated', '01-Jan-1970', '31-Dec-2099', NULL),
    (4, 'Ethnicity', 'Domain', 'Domain', 'Domain', NULL, 'OMOP generated', '01-Jan-1970', '31-Dec-2099', NULL);

INSERT INTO concept_class VALUES
    ('Domain', 'Domain', 1),
    ('Gender', 'Gender', 2),
    ('Race', 'Race', 3),
    ('Ethnicity', 'Ethnicity', 4);

INSERT INTO vocabulary VALUES
    ('Domain', 'Domain', NULL, NULL, 1),
    ('Gender', 'Gender', NULL, NULL, 2),
    ('Race', 'Race', NULL, NULL, 3),
    ('Ethnicity', 'Ethnicity', NULL, NULL, 4);
    
INSERT INTO domain VALUES
    ('Domain', 'Domain', 1),
    ('Gender', 'Gender', 2),
    ('Race', 'Race', 3),
    ('Ethnicity', 'Ethnicity', 4);