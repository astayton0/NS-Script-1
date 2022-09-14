/**
 * Module Description
 *    -
 * Version    Date            Author           Remarks
 * 1.00       21 Feb 2022     Brandon Fuller   The Vested Group - Initial Version
 *
 *@NApiVersion 2.x
 *@NScriptType MapReduceScript
 */
var modules = ["N/search", "N/record", "N/runtime", "N/sftp", "N/email", "N/file"];
define(modules, function(search, record, runtime, sftp, email, file) {

    // Grab connection related script parameters
    var script = runtime.getCurrentScript();
    var username = script.getParameter("custscript_twm_username");
    var domainName = script.getParameter("custscript_twm_domain");
    var hostKey = script.getParameter("custscript_twm_hostkey");
    var directory = script.getParameter("custscript_twm_po_directory");
    var guid = script.getParameter("custscript_twm_guid");

    // Establish the connection with TWM's SFTP site.
    function getInputData() {
        var connection = sftp.createConnection({
            username: username,
            url: domainName,
            passwordGuid : guid,
            hostKey: hostKey,
            hostKeyType: 'rsa',
            port : 22
        });
        // Return the list of files in the TWM outbound directory
        return connection.list({
            path : directory,
            sort : sftp.Sort.DATE
        });
        // return [12345];
    }

    function map(context) {
        log.audit("mapContext", context);

        // Grab the file name
        var fileName = JSON.parse(context.value).name;
        try {
            var connection = sftp.createConnection({
                username: username,
                url: domainName,
                passwordGuid : guid,
                hostKey: hostKey,
                hostKeyType: 'rsa',
                port : 22
            });
            log.audit("filename", fileName);
            log.audit("directory", directory);
            // Download the file
            var fileObj = connection.download({
                filename : fileName,
                directory : directory
            });
            log.audit("fileObj", JSON.stringify(fileObj));
            // Initialize the lookup object, used to set 'select' type fields.
            var lines = fileObj.lines.iterator();
            var lookupObj = runLookups(lines);
            log.audit("lines.length", lines.length);
            // Create the Sales Order transaction

            var headerSet = false;
            lines = fileObj.lines.iterator();
            // Iterate through the lines to set values on the transaction
            var count = 0;
            var soRec;
            var subsidiary;
            var inactives = [];
            lines.each(function(line) {
                log.debug("line", line);
                var lineValues = line.value.split(",");
                log.debug("lineValues", JSON.stringify(lineValues));
                // Skip the header row.
                if (count != 0) {
                    // Set header field values if they have not already been set
                    if (!headerSet) {
                        var storeNum = lineValues[6];
                        log.audit("storeNum", storeNum);
                        soRec = record.create({
                            type : "salesorder",
                            isDynamic : true,
                            defaultValues : {
                                entity : lookupObj.customers[storeNum]
                            }
                        });
                        //soRec.setValue("entity", lookupObj.customers[storeNum]);
                        soRec.setValue("trandate", new Date(lineValues[16]));
                        soRec.setValue("shipdate", new Date(lineValues[15]));
                        soRec.setValue("otherrefnum", lineValues[13]);
                        soRec.setValue("orderstatus", "A"); // default to Pending Approval
                        headerSet = true;
                        subsidiary = soRec.getValue("subsidiary");
                    }

                    var itemNum = lineValues[19];
                    log.audit("itemNum, lookupObj", itemNum + " : " + JSON.stringify(lookupObj));
                    // Select a new line and set values, referencing the lookupObj for internal ids
                    if (!(lookupObj.items[itemNum])) {
                        inactives.push({
                            itemNum : lineValues[20],
                            itemId : null,
                            poNum : lineValues[13]
                        });
                    } else if (lookupObj.items[itemNum].inactive) {
                        /*context.write("inactive", {
                            itemNum : itemNum,
                            itemId : lookupObj.items[itemNum].id,
                            poNum : lineValues[13]
                        });*/
                        inactives.push({
                            itemNum : itemNum,
                            itemId : lookupObj.items[itemNum].id,
                            poNum : lineValues[13]
                        });
                    } else {
                        soRec.selectNewLine("item");
                        log.audit("itemNum", itemNum);
                        soRec.setCurrentSublistValue("item", "item", lookupObj.items[itemNum].id);
                        var quantity = lineValues[26];
                        soRec.setCurrentSublistValue("item", "quantity", quantity);
                        if (subsidiary == 2) {
                            var amount = parseFloat(lineValues[33]);
                            var rate = amount / quantity
                            soRec.setCurrentSublistValue("item", "price", -1); // custom price level
                            soRec.setCurrentSublistValue("item", "rate", rate);
                            soRec.setCurrentSublistValue("item", "amount", amount);
                        }
                        soRec.commitLine("item");
                    }
                } else {
                    count++;
                }

                return true;
            });

            // Save the Sales Order and lookup the Document Number and Total
            var soId = soRec.save();

            // Move the file on the SFTP site to the archive directory
            var archive = script.getParameter("custscript_twm_po_archive");
            var fromPath = directory + "/" + fileName;
            var toPath = archive + fileName;
            log.emergency("fromPath", fromPath);
            log.emergency("toPath", toPath);
            connection.move({
                from : fromPath,
                to : toPath
            });

            var soNumberLookup = search.lookupFields({
                type : "salesorder",
                id : soId,
                columns : ["tranid", "total", "salesrep", "customer.custentity34"]
            });
            log.audit("soNumberLookup", JSON.stringify(soNumberLookup));
            // Write with the "success" key to pass email information
            context.write("success", {
                "soId" : soId,
                "tranid" : soNumberLookup.tranid,
                "total" : soNumberLookup.total,
                "salesRep" : soNumberLookup["salesrep"][0].value,
                "chainManager" : soNumberLookup["customer.custentity34"][0].value,
                "inactives" : inactives
            });
        } catch(e) {

            // Log the error and write with the "error" key to pass email information
            log.error("Map Error:", JSON.stringify(e));
            context.write("error", {
                "filename" : fileName,
                "error" : e
            });
        }
    }

    function runLookups(lines) {

        var lookupObj = {
            "items" : {},
            "customers" : {}
        };
        // Iterate through the lines and initialize each item and location key
        var count = 0;
        lines.each(function(line) {
            if (count != 0) {
                log.audit("line-", line);
                var lineValues = line.value.split(",");
                log.audit("lineValues", lineValues);
                var twmItemNumber = lineValues[18];
                var nsItemNumber = lineValues[19];
                // lookupObj.items[twmItemNumber] = null;
                lookupObj.items[nsItemNumber] = null;
                var twmStoreNumber = lineValues[6];
                log.audit("twmItemNumber,twmStoreNumber", twmItemNumber + ", " + twmStoreNumber);
                lookupObj.customers[twmStoreNumber] = null;
            } else {
                count++;
            }
            return true;
        });
        log.audit("lookupObj 1", JSON.stringify(lookupObj));

        // Get the array of item names from the lookupObj keys
        var items = Object.keys(lookupObj.items);

        // Create the search filter expression
        var itemFilters = [];
        items.forEach(function(item) {
            itemFilters.push(["name", "is", item], "OR"); // custitem987659002
        })
        itemFilters.pop();

        // Run the item search for internal ids
        var itemSearch = search.create({
            type : "item",
            filters : itemFilters,
            columns : [
                search.createColumn({name : "formulatext", formula : "LTRIM(regexp_substr({itemid},'[^:]*$'))"}),
                search.createColumn({name : "isinactive"})
            ] // custitem987659002
        });
        var itemResults = itemSearch.run().getRange(0,1000);
        log.audit("itemResults", JSON.stringify(itemResults));

        // Populate the item internal id in the lookupObj using the item name key
        itemResults.forEach(function(result) {
            var itemName =  result.getValue(itemSearch.columns[0]);
            if (itemName in lookupObj.items) {
                lookupObj.items[itemName] = {
                    id : result.id,
                    inactive : result.getValue("isinactive")
                }
            }
        });

        var customers = Object.keys(lookupObj.customers);
        var customerFilters = [["custentity29", "anyof", 41], "AND"];
        customers.forEach(function(customer) {
            customerFilters.push(["custentity11", "is", customer], "OR");
        })
        customerFilters.pop();
        var customerSearch = search.create({
            type : "customer",
            filters : customerFilters,
            columns : [search.createColumn({name : "custentity11"})]
        });
        var customerResults = customerSearch.run().getRange(0,1000);
        log.audit("customerResults", JSON.stringify(customerResults));

        customerResults.forEach(function(result) {
            var customerName =  result.getValue(customerSearch.columns[0]);
            if (customerName in lookupObj.customers) {
                lookupObj.customers[customerName] = result.id;
            }
        });
        log.audit("lookupObj 2", JSON.stringify(lookupObj));

        return lookupObj;
    }

    function summarize(summary) {
        log.debug('Summary Time','Total Seconds: '+summary.seconds);
        log.debug('Summary Usage', 'Total Usage: '+summary.usage);
        log.debug('Summary Yields', 'Total Yields: '+summary.yields);
        log.debug('Map Summary: ', JSON.stringify(summary.mapSummary));

        // Initialize success and error html rows
        var successRows = "";
        var errorRows = "";
        var inactiveRows = "";
        var inactiveObj = {};

        // Iterate through the summary output
        summary.output.iterator().each(function(key, value) {
            var values = JSON.parse(value);

            // Check the key value for successes and errors
            if (key === "success") {

                // Format a link to the successfully created Sales Order
                var url = '/app/accounting/transactions/salesord.nl?id=' + values.soId;
                var link = "<a href='"+url+"'>" + values.tranid + "</a>";
                // Add the row html to the successRows string
                successRows += "<tr><td>"+link+"</td><td>"+values.total+"</td></tr>";

                var salesRep = values.salesRep;
                var chainManager = values.chainManager;
                var orderLink = "<a href='/app/accounting/transactions/salesord.nl?id="+values.soId+"'>"+values.tranid+"</a>";
                var inactives = values.inactives;
                log.audit("salesRep, chainManager", salesRep + ", " + chainManager);
                log.audit("inactives", inactives);
                for (var x = 0; x < inactives.length; x++) {
                    var itemId = inactives[x].itemId;
                    var itemNum = inactives[x].itemNum;
                    var itemLink = "<a href='/app/common/item/item.nl?id="+itemId+"'>"+itemNum+"</a>";
                    if (!itemId) {
                        itemLink = itemNum + " (May not exist)";
                    }
                    if (salesRep) {
                        if (!(salesRep in inactiveObj)) {
                            inactiveObj[salesRep] = [];
                        }
                        inactiveObj[salesRep].push({
                            itemLink : itemLink,
                            orderLink : orderLink
                        });
                    }
                    if (chainManager && chainManager !== salesRep) {
                        if (!(chainManager in inactiveObj)) {
                            inactiveObj[chainManager] = [];
                        }
                        inactiveObj[chainManager].push({
                            itemLink : itemLink,
                            orderLink : orderLink
                        });
                    }
                }


            } else if (key === "error") {
                log.error("values", JSON.stringify(values));
                // Add the row html to the errorRows string
                errorRows += "<tr><td>" + values.filename + "</td><td>" + JSON.stringify(values.error) + "</td></tr>";

            }
            return true;
        });
        log.audit('inactiveObj', JSON.stringify(inactiveObj));
        if (Object.keys(inactiveObj).length > 0) {
            for (var emailRecip in inactiveObj) {
                var inactiveBody = "<table><tr><th colspan='2'>The following inactive items were ordered:</th></tr>" +
                    "<tr><th>Item</th><th>Order</th></tr>";
                var inactive = inactiveObj[emailRecip];
                inactive.forEach(function(item) {
                    var inactiveRow = "<tr><td>"+item.itemLink+"</td><td>"+item.orderLink+"</td></tr>"
                    inactiveBody += inactiveRow;
                    inactiveRows += inactiveRow;
                });
                inactiveBody += "</table>";
                email.send({
                    author : emailRecip,
                    recipients : [emailRecip],
                    subject : "Inactive Items on Total Wine Order Request",
                    body : inactiveBody
                });
            }
        }

        // Check that there is content in either the success or error rows
        if (successRows + errorRows + inactiveRows) {

            // Initialize the success table string
            var successTable = "";
            if (successRows) {
                // Add table header rows and contents
                successTable += "<table>" +
                    "<tr><th colspan='2'></th>Sales Orders successfully created from TWM Purchase Requests:</tr>" +
                    "<tr><th>Sales Order</th><th>Total</th></tr>" +
                    successRows +
                    "</table>";
            }

            // Initialize the error table string
            var errorTable = "";
            if (errorRows) {
                // Add table header rows and contents
                errorTable += "<table>" +
                    "<tr><th colspan='2'>Errors occurred while processing TWM Purchase Requests:</th></tr>" +
                    "<tr><th>File Name</th><th>Error</th></tr>" +
                    errorRows +
                    "</table>";
            }

            var inactiveTable = "";
            if (inactiveRows) {
                inactiveTable += "<table>" +
                    "<tr><th colspan='2'>The following inactive items were ordered:</th></tr>" +
                    "<tr><th>Item</th><th>Order</th></tr>" +
                    inactiveRows +
                    "</table>";
            }

            // Send the consolidated email
            email.send({
                author : script.getParameter("custscript_twm_email_author"),
                recipients : script.getParameter("custscript_twm_email_recipient").split(","),
                subject : "TWM Purchase Request files processed: " + new Date(),
                body : successTable + errorTable + inactiveTable
            });
        }
    }

    return {
        getInputData : getInputData,
        map : map,
        summarize : summarize
    }
});
