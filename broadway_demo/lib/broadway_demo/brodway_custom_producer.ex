defmodule BroadwayDemo.BroadwayCustomProducer do
  @moduledoc false

  use Broadway

  alias Broadway.Message

  def start_link(opts) do
    IO.puts(" Démarrage du pipeline BroadwayCustomProducer…")

    Broadway.start_link(__MODULE__,
      name: __MODULE__,
      # Kafka rbitmq, sqs et autres
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

  # Producteur → Processor (handle_message) → Batcher A → handle_batch(:batcher_a)
  #                                   ↘ Batcher B → handle_batch(:batcher_b)

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

  # traite chaque message unitairement dès sa sortie du producteur et les regroupe dans un batcher. Des que le nombrede message est attent alors ils sont envoyer a hendel_batch
  def handle_message(_, %Broadway.Message{data: %{event: {:ok, taxidata}}} = message, _) do
    IO.puts("#{inspect(self())} Handling first step: #{inspect(taxidata)}")
    message = Broadway.Message.update_data(message, fn _data -> taxidata end)
    IO.inspect(taxidata["ride_status"], label: "Ride status")

    case taxidata["ride_status"] do
      "enroute" ->
        Broadway.Message.put_batcher(message, :enroute)

      "pickup" ->
        Broadway.Message.put_batcher(message, :pickup)

      "dropoff" ->
        Broadway.Message.put_batcher(message, :dropoff)

      _ ->
        ## mark the message as failed
        Broadway.Message.failed(message, "unknown-status")
    end
  end

  def handle_message(_, %Broadway.Message{data: %{event: {:error, _}}} = message, _) do
    IO.puts("Error in message: #{inspect(message)}")
    Broadway.Message.failed(message, "unknown-status")
  end

  # acknowledge messages that failed we can alwo resend them
  def handle_failed(messages, _) do
    IO.puts("messages in faild stage: #{inspect(messages)}")

    Enum.map(messages, fn
      %{status: %{failed: "unknown-status"}} = message ->
        # on indique au produceuer ces messages ont été correctement consommés et n’ont pas besoin d’être renvoyés.
        Broadway.Message.configure_ack(message, on_failure: :ack)

      message ->
        message
    end)
  end

  ## traite un lot de message, chaque batch peut envoyer les donnée qu'il a traité a une bdd ou en api ...
  def handle_batch(:enroute, messages, batch_info, _context) do
    # les message renvoyer par un batcher sont par defaut l’ack on a pas besoins d'ecrir du code pour tanque handel_batch ne renvoie pas d'erreur.Broadway considérera que l’ensemble du lot a bien été traitée et donc le produceur  sait que le lot de message a été consomé
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
