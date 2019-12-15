\echo Creating table "topic"...
create table topic
(
    id      serial primary key,
    name    text not null check (name <> '')
);

comment on table topic is 'Define a topic';
comment on column topic.id is 'Primary key for a topic';
comment on column topic.name is 'Name of the topic';


\echo Creating table "topic_message"...
create table topic_message
(
    topic_id    int not null references topic (id) on delete cascade deferrable initially deferred,
    _offset     bigint not null check (_offset > 0),
    message     text not null,
    primary key (topic_id, _offset)
);

comment on table topic_message is 'Messages posted to a topic';
comment on column topic_message.topic_id is 'Reference to the topic to which this message applies. Part 1 of the primary key';
comment on column topic_message._offset is 'Offset identifier for the topic message. Part 2 of the primary key';
comment on column topic_message.message is 'Payload of the message';


\echo Creating table "topic_subscriber"...
create table topic_subscriber
(
    id          serial primary key,
    topic_id    int not null,
    _offset      bigint not null default 0,
    foreign key (topic_id, _offset) references topic_message (topic_id, _offset)
);

comment on table topic_subscriber is 'Define a topic subscriber';
comment on column topic_subscriber.id is 'Primary key for a topic subscriber';
comment on column topic_subscriber.topic_id is 'Identify the subscribed topic. Part 1 of foreign key.';
comment on column topic_subscriber._offset is 'Last processed offset of subscribed topic. Part 2 of foreign key.';


