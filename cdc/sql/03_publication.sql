CREATE PUBLICATION :"publication_name"
    FOR TABLE :"login".recipients
    WITH (publish = 'insert, update, delete');

