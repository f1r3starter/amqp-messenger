<?php

/*
 * This file is part of the Symfony package.
 *
 * (c) Fabien Potencier <fabien@symfony.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

namespace Symfony\Component\Messenger\Bridge\RabbitMQ\Transport;

use Symfony\Component\Messenger\Transport\AmqpExt\AmqpReceivedStamp;
use Symfony\Component\Messenger\Transport\AmqpExt\Connection;
use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Exception\LogicException;
use Symfony\Component\Messenger\Exception\MessageDecodingFailedException;
use Symfony\Component\Messenger\Exception\TransportException;
use Symfony\Component\Messenger\Transport\Receiver\MessageCountAwareInterface;
use Symfony\Component\Messenger\Transport\Receiver\ReceiverInterface;
use Symfony\Component\Messenger\Transport\Serialization\PhpSerializer;
use Symfony\Component\Messenger\Transport\Serialization\SerializerInterface;

/**
 * Symfony Messenger receiver to get messages from AMQP brokers using PHP's AMQP extension.
 *
 * @author Samuel Roze <samuel.roze@gmail.com>
 */
class RabbitMQReceiver implements ReceiverInterface, MessageCountAwareInterface
{
    private $serializer;
    private $connection;
    private $messages = [];

    public function __construct(Connection $connection, SerializerInterface $serializer = null)
    {
        $this->connection = $connection;
        $this->serializer = $serializer ?? new PhpSerializer();
    }

    /**
     * {@inheritdoc}
     */
    public function get(): iterable
    {
        foreach ($this->connection->getQueueNames() as $queueName) {
            $this->getEnvelope($queueName);
            yield from $this->messages[$queueName];
        }
    }

    private function getEnvelope(string $queueName): void
    {
        try {
            $this->connection
                ->queue($queueName)
                ->consume(
                    function (\AMQPEnvelope $amqpEnvelope) use ($queueName): void {
                        $this->consume($amqpEnvelope, $queueName);
                    }
                );
        } catch (\AMQPException $exception) {
            throw new TransportException($exception->getMessage(), 0, $exception);
        }
    }

    public function consume(\AMQPEnvelope $amqpEnvelope, string $queueName): void
    {
        $body = $amqpEnvelope->getBody();

        try {
            $this->messages[$queueName] = $this->serializer->decode(
                [
                    'body' => false === $body ? '' : $body,
                    // workaround https://github.com/pdezwart/php-amqp/issues/351
                    'headers' => array_merge(
                        $amqpEnvelope->getHeaders(),
                        [
                            'type' => $amqpEnvelope->getType(),
                            'content_type' => $amqpEnvelope->getContentType(),
                            'app_id' => $amqpEnvelope->getAppId()
                        ]
                    ),
                ]
            )->with(new AmqpReceivedStamp($amqpEnvelope, $queueName));
        } catch (MessageDecodingFailedException $exception) {
            // invalid message of some type
            $this->rejectAmqpEnvelope($amqpEnvelope, $queueName);

            throw $exception;
        }
    }

    /**
     * {@inheritdoc}
     */
    public function ack(Envelope $envelope): void
    {
        try {
            $stamp = $this->findAmqpStamp($envelope);

            $this->connection->ack(
                $stamp->getAmqpEnvelope(),
                $stamp->getQueueName()
            );
        } catch (\AMQPException $exception) {
            throw new TransportException($exception->getMessage(), 0, $exception);
        }
    }

    /**
     * {@inheritdoc}
     */
    public function reject(Envelope $envelope): void
    {
        $stamp = $this->findAmqpStamp($envelope);

        $this->rejectAmqpEnvelope(
            $stamp->getAmqpEnvelope(),
            $stamp->getQueueName()
        );
    }

    /**
     * {@inheritdoc}
     */
    public function getMessageCount(): int
    {
        try {
            return $this->connection->countMessagesInQueues();
        } catch (\AMQPException $exception) {
            throw new TransportException($exception->getMessage(), 0, $exception);
        }
    }

    private function rejectAmqpEnvelope(\AMQPEnvelope $amqpEnvelope, string $queueName): void
    {
        try {
            $this->connection->nack($amqpEnvelope, $queueName, \AMQP_NOPARAM);
        } catch (\AMQPException $exception) {
            throw new TransportException($exception->getMessage(), 0, $exception);
        }
    }

    private function findAmqpStamp(Envelope $envelope): AmqpReceivedStamp
    {
        $amqpReceivedStamp = $envelope->last(AmqpReceivedStamp::class);
        if (null === $amqpReceivedStamp) {
            throw new LogicException('No "AmqpReceivedStamp" stamp found on the Envelope.');
        }

        return $amqpReceivedStamp;
    }
}