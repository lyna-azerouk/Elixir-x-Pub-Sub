defmodule BroadwayDemo.BroadwayCustomProducer do
  @moduledoc false

  # Producteur → Processor (handle_message) → Batcher A → handle_batch(:batcher_a)
  #                                   ↘ Batcher B → handle_batch(:batcher_b)

  use Broadway

  alias Broadway.Message

  def start_link(opts) do
    IO.puts(" Démarrage du pipeline BroadwayCustomProducer…")

    Broadway.start_link(__MODULE__,
      name: __MODULE__,
      # PubSub, Kafka, RabitMq, etc
      producer: [
        module:
          {BroadwayCloudPubSub.Producer,
           goth: BroadwayDemo.Goth,
           subscription: "projects/my-broadway-project/subscriptions/my-app-sub"},
        concurrency: 1
      ],
      processors: [
        default: [
          concurrency: 6,
          partition_by: &partition/1
        ]
      ],
      batchers: [
        pickup: [
          concurrency: 1,
          batch_size: 5,
          batch_timeout: 1000
        ],
        dropoff: [
          concurrency: 1,
          batch_size: 5,
          batch_timeout: 1000
        ],
        enroute: [
          concurrency: 4,
          batch_size: 25,
          batch_timeout: 2000
        ]
      ]
    )
  end

  def prepare_messages(messages, _context) do
    Enum.map(messages, fn msg ->
      Broadway.Message.update_data(msg, fn data ->
        case Jason.decode(data) do
          {:ok, decoded} -> %{event: {:ok, decoded}}
          error -> %{event: error}
        end
      end)
    end)
  end

  @doc """
    # Processes each message individually as it comes out of the producer and groups them into a batcher.
    # As soon as the required number of messages is reached, they are sent to handle_batch.
  """
  def handle_message(_, %Broadway.Message{data: %{event: {:ok, taxidata}}} = message, _) do
    IO.puts("#{inspect(self())} Handling first step: #{inspect(taxidata)}")
    message = Broadway.Message.update_data(message, fn _data -> taxidata end)

    case taxidata["ride_status"] do
      "enroute" ->
        Broadway.Message.put_batcher(message, :enroute)

      "pickup" ->
        Broadway.Message.put_batcher(message, :pickup)

      "dropoff" ->
        Broadway.Message.put_batcher(message, :dropoff)

      _ ->
        # mark the message as failed
        Broadway.Message.failed(message, "unknown-status")
    end
  end

  def handle_message(_, %Broadway.Message{data: %{event: {:error, _}}} = message, _) do
    IO.puts("Error in message: #{inspect(message)}")
    Broadway.Message.failed(message, "unknown-status")
  end

  @doc """
    # Acknowledge messages that failed
    # This function is called when a message fails to be processed.
  """
  def handle_failed(messages, _) do
    IO.puts("messages in faild stage: #{inspect(messages)}")

    Enum.map(messages, fn
      %{status: %{failed: "unknown-status"}} = message ->
        # Inform the producer that these messages have been successfully consumed and do not need to be redelivered.
        Broadway.Message.configure_ack(message, on_failure: :ack)

      message ->
        message
    end)
  end

  @doc """
    Processes a batch of messages; each batch can send the data it has processed to a database or via an API …
  """
  def handle_batch(:enroute, messages, batch_info, _context) do
    # Messages returned by a batcher are automatically acknowledged by default; you don’t need to write any code as long as handle_batch doesn’t return an error. Broadway will consider the entire batch successfully processed, and the producer will know that the batch of messages has been consumed.
    IO.puts("Enroute batch: #{inspect(batch_info)}")

    list = Enum.map(messages, fn message -> message.data end)

    IO.inspect(list, label: "Got enroute messages")
    messages
  end

  def partition(msg) do
    case Jason.decode(msg.data) do
      {:ok, data} ->
        :erlang.phash2(data["ride_id"])

      _ ->
        0
    end
  end
end
