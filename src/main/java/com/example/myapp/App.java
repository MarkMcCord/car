package com.example.myapp;

// snippet-start:[s3.java2.list_objects.import]
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.ListObjectsRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.S3Object;
import java.util.List;
import java.util.ListIterator;
// snippet-end:[s3.java2.list_objects.import]

// snippet-start:[rekognition.java2.detect_labels.import]
import software.amazon.awssdk.services.rekognition.RekognitionClient;
import software.amazon.awssdk.services.rekognition.model.Image;
import software.amazon.awssdk.services.rekognition.model.DetectLabelsRequest;
import software.amazon.awssdk.services.rekognition.model.DetectLabelsResponse;
import software.amazon.awssdk.services.rekognition.model.Label;
import software.amazon.awssdk.services.rekognition.model.RekognitionException;
// snippet-end:[rekognition.java2.detect_labels.import]

// snippet-start:[sqs.java2.send_recieve_messages.import]
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;

// snippet-end:[sqs.java2.send_recieve_messages.import]


/**
 * To run this AWS code example, ensure that you have setup your development environment, including your AWS credentials.
 *
 * For information, see this documentation topic:
 *
 * https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/get-started.html
 */

public class App {

    public static void main(String[] args) {

        final String USAGE = "\n" +
                "Usage:\n" +
                "    <bucketName> \n\n" +
                "Where:\n" +
                "    bucketName - the Amazon S3 bucket from which objects are read. \n\n" ;

       if (args.length != 1) {
           System.out.println(USAGE);
           System.exit(1);
        }

        String bucketName = args[0];
        Region region = Region.US_WEST_2;


        S3Client s3 = S3Client.builder()
                .region(region)
                .build();
        RekognitionClient rekClient = RekognitionClient.builder()
                .region(region)
                .build();
        SqsClient sqsClient = SqsClient.builder()
                .region(Region.US_EAST_2)
                .build();
        
            String queueUrl = "https://sqs.us-east-2.amazonaws.com/537983741076/fifoQueue.fifo";
                    /*
                    // Receive messages from the queue
                    ReceiveMessageRequest receiveRequest = ReceiveMessageRequest.builder()
                        .queueUrl(queueUrl)
                        .build();
                    List<Message> messages = sqsClient.receiveMessage(receiveRequest).messages();
        
                    // Print out the messages
                     for (Message m : messages) {
                        System.out.println("\n" +m.body());
                    }*/

        listBucketObjects(s3, bucketName, rekClient, sqsClient, queueUrl);

        SendMessageRequest sendMsgRequest = SendMessageRequest.builder()
        .queueUrl(queueUrl)
        .messageGroupId("fifo")
        .messageBody("-1")
        .build();
        sqsClient.sendMessage(sendMsgRequest);

        s3.close();
        rekClient.close();
        sqsClient.close();
    }

    // snippet-start:[s3.java2.list_objects.main]
    public static void listBucketObjects(S3Client s3, String bucketName, RekognitionClient rekClient, SqsClient sqsclient, String queueUrl) {

       try {
            ListObjectsRequest listObjects = ListObjectsRequest
                    .builder()
                    .bucket(bucketName)
                    .build();

            ListObjectsResponse res = s3.listObjects(listObjects);
            List<S3Object> objects = res.contents();

            for (ListIterator<S3Object> iterVals = objects.listIterator(); iterVals.hasNext(); ) {
                S3Object myValue = (S3Object) iterVals.next();
                System.out.print("\n The name of the key is " + myValue.key() + "\n");
                detectImageLabels(rekClient, myValue, bucketName, sqsclient, queueUrl);
             }

        } catch (S3Exception e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
    }

   // snippet-end:[s3.java2.list_objects.main]

    // snippet-start:[rekognition.java2.detect_labels.main]
    public static void detectImageLabels(RekognitionClient rekClient, S3Object sourceImage, String bucketName, SqsClient sqsClient, String queueUrl) {

        try {
   
        // InputStream sourceStream = new URL("https://images.unsplash.com/photo-1557456170-0cf4f4d0d362?ixid=MnwxMjA3fDB8MHxzZWFyY2h8MXx8bGFrZXxlbnwwfHwwfHw%3D&ixlib=rb-1.2.1&w=1000&q=80").openStream();
        //InputStream sourceStream = new FileInputStream(sourceImage);
        //SdkBytes sourceBytes = SdkBytes.fromInputStream(sourceStream);
   
        software.amazon.awssdk.services.rekognition.model.S3Object s3Object = software.amazon.awssdk.services.rekognition.model.S3Object.builder()
            .bucket(bucketName)
            .name(sourceImage.key())
            .build();

        // Create an Image object for the source image.
        Image souImage = Image.builder()
            .s3Object(s3Object)
            .build();
   
        DetectLabelsRequest detectLabelsRequest = DetectLabelsRequest.builder()
            .image(souImage)
            .maxLabels(10)
            .build();
   
        DetectLabelsResponse labelsResponse = rekClient.detectLabels(detectLabelsRequest);
        List<Label> labels = labelsResponse.labels();
   
        //System.out.println("Detected labels for the given photo");
        for (Label label: labels) {
            if(label.name().equals("Car") && label.confidence() > 90){
                System.out.println(label.name() + ": " + label.confidence().toString());
                SendMessageRequest sendMsgRequest = SendMessageRequest.builder()
                .queueUrl(queueUrl)
                .messageGroupId("fifo")
                .messageBody(sourceImage.key())
                .build();
            sqsClient.sendMessage(sendMsgRequest);
            }
        }
   
    } catch (RekognitionException e) {
        System.out.println(e.getMessage());
        System.exit(1);
    }
    }
        // snippet-end:[rekognition.java2.detect_labels.main]
}