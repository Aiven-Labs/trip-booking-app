# trip-booking-app
Trip Booking App, demo with Aiven Kafka

Through Aiven Kafka and Ververica Flink, we can personalize the customer’s user experience with target suggestions based on browsing history, length of stay, and determining if the trip is potentially a family holiday or a business event.

DEMO ACTIONS:

Input/Select the following from the Form:

Location: Austin
Check in and check out: Any date range in the future

Guests: 1

Submit form data

Upon submission, several actions will take place.  Based on logic in our application, an LLM attached to the streaming backend (Kafka->Flink->LLM) will generate a prompt to apply real-time context (where they have been browsing on the Priceline site for flights, hotels, car rental) and return a suggested itinerary.  Due to the fact we see a single guest, we will prompt the LLM to return activities based on a “business travel” classification.  Additionally, the customer will see a “Booking” form appear, with the recommended hotels available to book.

However, maybe this traveler decides to book for their entire family and stay a few days.  

CHANGE the guests to 4 and resubmit the form

Now, receiving the inputs in real-time, we can prompt the LLM to produce content relevant to a Family trip and provide activities the customer may want to experience for this trip.  Again, the booking screen will provide a selection of options to book their hotel.
