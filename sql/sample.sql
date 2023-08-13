create or REPLACE language plpgsql;

$$
begin
    if not exists(schema asyncpg) then create schema asyncpg;
end
$$ language plpgsql;

create function test_runner(tph anyelement, q text) returns setof anyelement as
$$
BEGIN
    return query execute q;
end;
$$ language plpgsql;

create function sync_runner(query text, session_id text) returns bool as
$$
begin
    execute 'create temp table ' || session_id || ' as  ' || query;
    return true;
exception
    when others then
        execute query;
        return false;
end;
$$ language plpgsql;