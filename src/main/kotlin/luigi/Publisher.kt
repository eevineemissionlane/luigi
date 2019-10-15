package luigi

import com.google.api.core.ApiFuture
import com.google.api.core.ApiFutures
import com.google.cloud.ServiceOptions
import com.google.cloud.pubsub.v1.Publisher
import com.google.protobuf.ByteString
import com.google.pubsub.v1.ProjectTopicName
import com.google.pubsub.v1.PubsubMessage

object Publisher {

    // Load projectId from local auth
    private val projectId = ServiceOptions.getDefaultProjectId()!!

    fun sendSamples(count: Int = 1) {

        val topicId = "luigi.ingress.dev"
        val topicName = ProjectTopicName.of(projectId, topicId)

        lateinit var publisher: Publisher

        val futures: MutableList<ApiFuture<String>> = mutableListOf()

        try {
            // Create a publisher instance with default settings bound to the topic
            // By default using authenticated user's cred
            // TODO: need service account key for application
            publisher = Publisher.newBuilder(topicName).build()

            for (i in 1..count) {
                val message = "message-$i"

                // convert message to bytes
                val data = ByteString.copyFromUtf8(message)
                val pubsubMessage = PubsubMessage.newBuilder()
                        .setData(data)
                        .build()

                // Schedule a message to be published. Messages are automatically batched.
                val future = publisher.publish(pubsubMessage)
                futures.add(future)
            }
        } finally {
            // Wait on any pending requests
            println("Published following messages (IDs):")

            val messageIds = ApiFutures.allAsList(futures).get()
            messageIds.forEach { messageId -> println(messageId) }

            publisher.shutdown()
        }
    }
}