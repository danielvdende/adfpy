# Custom Activities
While we would like to include support for all ADF activities out of the box, we also would like to offer the capability to the users of adfPy to add and customize their own adfPy components. To do this, the easiest way is to check out the source code of the existing AdfActivities.

To create a new AdfActivity, in a nutshell, you need to:

1. Create a class that extends `AdfActivity`
2. Ensure you correctly implement the `to_adf` method that converts all necessary attributes to ADF objects.
 
That's it! 