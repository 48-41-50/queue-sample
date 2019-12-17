\echo Creating table "topic"...
create table topic
(
    topic       text primary key check (topic <> '')
    description text not null check ( description <> '' );
);

comment on table topic is 'Define a topic';
comment on column topic.name is 'Name of the topic';


\echo Creating table "topic_message"...
create table topic_message
(
    id          bigserial primary key,
    topic       int not null check (topic <> '') references topic (topic) on delete cascade on update cascade deferrable initially deferred,
    _offset     bigint not null check (_offset > 0),
    message     text not null
);

create unique index topic_message_topic_id_offset_desc on topic_message (topic_id, _offset desc);

comment on table topic_message is 'Messages posted to a topic';
comment on column topic_message.topic is 'Reference to the topic to which this message applies. Part 1 of the primary key';
comment on column topic_message._offset is 'Offset identifier for the topic message. Part 2 of the primary key';
comment on column topic_message.message is 'Payload of the message';


\echo Creating table "topic_subscriber"...
create table topic_subscriber
(
    id          serial primary key,
    topic       text not null check (topic <> '') references topic (topic) on delete cascade deferrable initially deferred,
    _offset     bigint not null default 0
);

comment on table topic_subscriber is 'Define a topic subscriber';
comment on column topic_subscriber.id is 'Primary key for a topic subscriber';
comment on column topic_subscriber.topic_id is 'Identify the subscribed topic. Part 1 of foreign key.';
comment on column topic_subscriber._offset is 'Last processed offset of subscribed topic. Part 2 of foreign key.';


\echo Creating function create topic...
create or replace function topic_create( topic text, description text ) returning text as 
$BODY$
declare
    v_topic text := null::text;
begin
    insert into topic (topic, description) values (topic, description) returning topic into v_topic;
    
    return v_topic;
end;
$BODY$
language plpgsql;


\echo Creating procedure delete topic...
create or replace procedure topic_delete( topic text ) as 
$BODY$
declare
    v_topic text := null::text;
begin
    execute $$delete
                from topic
               where topic = $$ || quote_literal(topic) || $$;$$;
end;
$BODY$
language plpgsql;


\echo Creating save_message procedure...
create or replace procedure save_message(topic text, message text) as $BODY$
declare
    v_offset int := null::int;
begin
    execute $$select _offset 
                from "topic_message" tm
               where tm.topic = $$ || quote_literal(topic) || $$
                 and tm._offset = (select coalesce(max(tm1._offset), 0)
                                     from "topic_message" tm1
                                    where tm1.topic_id = $$ || quote_literal(v_topic_id) || $$)
                 for update;$$
       into v_offset;

    insert into "topic_message" (topic, _offset, message)
    values (topic, coalesce(v_offset, 0) + 1, quote_literal(message));
    
    commit;
end;
$BODY$
language plpgsql;


\echo Creating subscribe function...
create or replace function topic_subscribe(topic text) returns int as 
$BODY$
declare
    v_subscriber_id int := null::int;
    v_offset int := null::int;
begin
    execute $$select coalesce(max(tm._offset), 0)
                from "topic_message" tm
               where tm.topic = $$ || quote_literal(topic) || $$;$$
       into v_offset;
    execute $$insert into "topic_subscriber" (topic, _offset)
              values (topic, v_offset)
              returning id;$$
       into v_subscriber_id;
    
    if ( v_subscriber_id is null )
    then
        raise exception 'Could not create a subscriber for topic %', topic;
    end if;
    
    return v_subscriber_id;
end;
$BODY$
language plpgsql returns null on null input;


\echo Creating unsubscribe procedure...
create or replace procedure unsubscribe(subscriber_id int) as 
$BODY$
begin
    execute $$delete 
                from "topic_subscriber"
               where id = $$ || quote_literal(subscriber_id) || $$;$$;
    
    commit;
end;
$BODY$
language plpgsql;


\echo Creating getter function...
create or replace function get_message(topic text, subscriber_id int, num_messages int = 1) returns setof topic_message as 
$BODY$
begin 
    if ( num_messages < 1 ) then
        num_messages = 1;
    end if;
    
    return query execute $$select tm.*
                             from "topic_message" tm
                             join "topic" t
                               on t.id = tm.topic_id
                             join "topic_subscriber" ts
                               on ts.topic_id = t.id
                            where ts.id = $$ || quote_literal(subscriber_id) || $$
                              and t.name = $$ || quote_literal(topic) || $$
                              and tm._offset > ts._offset
                            limit $$ || quote_literal(num_messages) || $$;$$;
end;
$BODY$
language plpgsql returns null on null input;


\echo Creating ack procedure...
create or replace procedure ack_message(subscriber_id int, _offset int) as 
$BODY$
begin
    execute $$update "topic_subscriber"
                 set _offset = $$ || quote_literal(_offset) || $$
               where id = $$ || quote_literal(subscriber_id) || $$;$$;
    commit;
end;
$BODY$
language plpgsql;


\echo Creating rewind procedure...
create or replace procedure topic_rewind(topic text, _offset int = 0) as 
$BODY$
begin
    execute $$update "topic_subscriber" ts
                 set _offset = $$ || quote_literal(_offset) || $$
                from (
                       select ts1.id
                         from "topic_subscriber" ts1
                         join "topic" t
                           on t.id = ts1.topic_id
                        where t.name = $$ || quote_literal(topic) || $$ 
                          for update
                     ) as targets
               where ts.id = targets.id;$$;
    commit;
end;
$BODY$
language plpgsql;


