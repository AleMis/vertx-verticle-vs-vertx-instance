# `Why you should use Verticles vs Vertx Instance while handlers could modify the same object`

According to the Vert.X documentation "Vert.x APIs are event driven - Vert.x passes events to handlers when they are available."

We also can read that "Vert.x calls handlers using a thread called an event loop." It means that Vertx instance maintains several event loops (By default we choose the number based on the number of available cores on the machine) which are used to resolve events called by handlers.

And few more information:
 1. "Vert.x comes with a simple, scalable, actor-like deployment and concurrency model"
 2. "To use this model, you write your code as set of verticles."
 3. "Standard verticles are assigned an event loop thread when they are created and the start method is called with that event loop. When you call any other methods that takes a handler on a core API from an event loop then Vert.x will guarantee that those handlers, when called, will be executed on the same event loop." 

The last and most important information is "When Vert.x provides an event to a handler or calls the start or stop methods of a Verticle, the execution is associated with a Context. Usually a context is an event-loop context and is tied to a specific event loop thread. So executions for that context always occur on that exact same event loop thread".
 
Why I wrote all of this? 
 
Because when you create application where:
* you have bean in your application which listen on two or more addresses, and
* handlers have access to the same object to modify it,
* you does not use Verticles, then

in high probability you will have problem with concurrency. It's because each on start Verticle will have assigned an event loop thread, so when your bean will be an verticle, then it will be called from the same event loop, even if it have few consumers. This guarantee that everything what is doing by your bean will be called in one thread. Without using the Verticle mechanism, you will have problem with concurrency.

To prove this word, have a look at the test "VerticleVsVertxInstanceTest" when you can find two tests which shows that when yours bean fulfill points mentioned above, verticles should be used instead of only Vertx instance. 