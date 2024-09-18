/**
 * NOTE: This class is auto generated by OpenAPI Generator (https://openapi-generator.tech) (7.8.0).
 * https://openapi-generator.tech
 * Do not edit the class manually.
 */
package org.databiosphere.workspacedataservice.generated;

import org.databiosphere.workspacedataservice.generated.DeleteRecordsRequestServerModel;
import org.databiosphere.workspacedataservice.generated.DeleteRecordsResponseServerModel;
import org.databiosphere.workspacedataservice.generated.EvaluateExpressionsRequestServerModel;
import org.databiosphere.workspacedataservice.generated.EvaluateExpressionsResponseServerModel;
import org.databiosphere.workspacedataservice.generated.EvaluateExpressionsWithArrayRequestServerModel;
import org.databiosphere.workspacedataservice.generated.EvaluateExpressionsWithArrayResponseServerModel;
import java.util.UUID;
import io.swagger.v3.oas.annotations.ExternalDocumentation;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.Parameters;
import io.swagger.v3.oas.annotations.media.ArraySchema;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.security.SecurityRequirement;
import io.swagger.v3.oas.annotations.tags.Tag;
import io.swagger.v3.oas.annotations.enums.ParameterIn;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.context.request.NativeWebRequest;
import org.springframework.web.multipart.MultipartFile;

import jakarta.validation.Valid;
import jakarta.validation.constraints.*;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import jakarta.annotation.Generated;

@Generated(value = "org.openapitools.codegen.languages.SpringCodegen", comments = "Generator version: 7.8.0")
@Validated
@Tag(name = "Record", description = "Record APIs")
public interface RecordApi {

    default Optional<NativeWebRequest> getRequest() {
        return Optional.empty();
    }

    /**
     * POST /records/v1/{collectionId}/{recordType}/delete : Bulk delete records
     * Deletes records by record ID, using collection ID. 
     *
     * @param collectionId Collection id (required)
     * @param recordType Record type (required)
     * @param deleteRecordsRequestServerModel Record deletion specification (required)
     * @return Deletion successful (status code 200)
     */
    @Operation(
        operationId = "deleteRecords",
        summary = "Bulk delete records",
        description = "Deletes records by record ID, using collection ID. ",
        tags = { "Record" },
        responses = {
            @ApiResponse(responseCode = "200", description = "Deletion successful", content = {
                @Content(mediaType = "application/json", schema = @Schema(implementation = DeleteRecordsResponseServerModel.class))
            })
        },
        security = {
            @SecurityRequirement(name = "bearerAuth")
        }
    )
    @RequestMapping(
        method = RequestMethod.POST,
        value = "/records/v1/{collectionId}/{recordType}/delete",
        produces = { "application/json" },
        consumes = { "application/json" }
    )
    
    default ResponseEntity<DeleteRecordsResponseServerModel> deleteRecords(
        @Parameter(name = "collectionId", description = "Collection id", required = true, in = ParameterIn.PATH) @PathVariable("collectionId") UUID collectionId,
        @Pattern(regexp = "[a-zA-Z0-9-_]{1,63}") @Parameter(name = "recordType", description = "Record type", required = true, in = ParameterIn.PATH) @PathVariable("recordType") String recordType,
        @Parameter(name = "DeleteRecordsRequestServerModel", description = "Record deletion specification", required = true) @Valid @RequestBody DeleteRecordsRequestServerModel deleteRecordsRequestServerModel
    ) {
        return new ResponseEntity<>(HttpStatus.NOT_IMPLEMENTED);

    }


    /**
     * POST /records/v1/{collectionId}/{recordType}/{recordId}/evaluateExpressions : Evaluate expressions on a single record
     * Evaluate expressions on a single record. The expression \&quot;this.foo\&quot; will get the value of the attribute \&quot;foo\&quot; in the record. The expression \&quot;this.relation.foo\&quot; will get the value of the attribute \&quot;foo\&quot; from the related record specified by attribute \&quot;relation\&quot; in the record. The expression \&quot;{&#39;name&#39;: this.foo, &#39;num&#39;: this.bar }\&quot; will create a JSON object with key \&quot;name\&quot; and value of the attribute \&quot;foo\&quot; and the key \&quot;num\&quot; and value of the attribute \&quot;bar. 
     *
     * @param collectionId Collection id (required)
     * @param recordType Record type (required)
     * @param recordId Record id (required)
     * @param evaluateExpressionsRequestServerModel Expressions to evaluate (required)
     * @return Results of evaluating expressions (status code 200)
     */
    @Operation(
        operationId = "evaluateExpressions",
        summary = "Evaluate expressions on a single record",
        description = "Evaluate expressions on a single record. The expression \"this.foo\" will get the value of the attribute \"foo\" in the record. The expression \"this.relation.foo\" will get the value of the attribute \"foo\" from the related record specified by attribute \"relation\" in the record. The expression \"{'name': this.foo, 'num': this.bar }\" will create a JSON object with key \"name\" and value of the attribute \"foo\" and the key \"num\" and value of the attribute \"bar. ",
        tags = { "Record" },
        responses = {
            @ApiResponse(responseCode = "200", description = "Results of evaluating expressions", content = {
                @Content(mediaType = "application/json", schema = @Schema(implementation = EvaluateExpressionsResponseServerModel.class))
            })
        },
        security = {
            @SecurityRequirement(name = "bearerAuth")
        }
    )
    @RequestMapping(
        method = RequestMethod.POST,
        value = "/records/v1/{collectionId}/{recordType}/{recordId}/evaluateExpressions",
        produces = { "application/json" },
        consumes = { "application/json" }
    )
    
    default ResponseEntity<EvaluateExpressionsResponseServerModel> evaluateExpressions(
        @Parameter(name = "collectionId", description = "Collection id", required = true, in = ParameterIn.PATH) @PathVariable("collectionId") UUID collectionId,
        @Pattern(regexp = "[a-zA-Z0-9-_]{1,63}") @Parameter(name = "recordType", description = "Record type", required = true, in = ParameterIn.PATH) @PathVariable("recordType") String recordType,
        @Parameter(name = "recordId", description = "Record id", required = true, in = ParameterIn.PATH) @PathVariable("recordId") String recordId,
        @Parameter(name = "EvaluateExpressionsRequestServerModel", description = "Expressions to evaluate", required = true) @Valid @RequestBody EvaluateExpressionsRequestServerModel evaluateExpressionsRequestServerModel
    ) {
        return new ResponseEntity<>(HttpStatus.NOT_IMPLEMENTED);

    }


    /**
     * POST /records/v1/{collectionId}/{recordType}/{recordId}/evaluateExpressionsWithArray : Evaluate expressions on array of records
     * Evaluate expressions on array of records. The expression \&quot;this.foo\&quot; will get the value of the attribute \&quot;foo\&quot; in each record. The expression \&quot;this.relation.foo\&quot; will get the value of the attribute \&quot;foo\&quot; from the related record specified by attribute \&quot;relation\&quot; in each record. The expression \&quot;{&#39;name&#39;: this.foo, &#39;num&#39;: this.bar }\&quot; will create a JSON object with key \&quot;name\&quot; and value of the attribute \&quot;foo\&quot; and the key \&quot;num\&quot; and value of the attribute \&quot;bar. 
     *
     * @param collectionId Collection id (required)
     * @param recordType Record type (required)
     * @param recordId Record id (required)
     * @param evaluateExpressionsWithArrayRequestServerModel Expressions to evaluate (required)
     * @return Results of evaluating expressions (status code 200)
     */
    @Operation(
        operationId = "evaluateExpressionsWithArray",
        summary = "Evaluate expressions on array of records",
        description = "Evaluate expressions on array of records. The expression \"this.foo\" will get the value of the attribute \"foo\" in each record. The expression \"this.relation.foo\" will get the value of the attribute \"foo\" from the related record specified by attribute \"relation\" in each record. The expression \"{'name': this.foo, 'num': this.bar }\" will create a JSON object with key \"name\" and value of the attribute \"foo\" and the key \"num\" and value of the attribute \"bar. ",
        tags = { "Record" },
        responses = {
            @ApiResponse(responseCode = "200", description = "Results of evaluating expressions", content = {
                @Content(mediaType = "application/json", schema = @Schema(implementation = EvaluateExpressionsWithArrayResponseServerModel.class))
            })
        },
        security = {
            @SecurityRequirement(name = "bearerAuth")
        }
    )
    @RequestMapping(
        method = RequestMethod.POST,
        value = "/records/v1/{collectionId}/{recordType}/{recordId}/evaluateExpressionsWithArray",
        produces = { "application/json" },
        consumes = { "application/json" }
    )
    
    default ResponseEntity<EvaluateExpressionsWithArrayResponseServerModel> evaluateExpressionsWithArray(
        @Parameter(name = "collectionId", description = "Collection id", required = true, in = ParameterIn.PATH) @PathVariable("collectionId") UUID collectionId,
        @Pattern(regexp = "[a-zA-Z0-9-_]{1,63}") @Parameter(name = "recordType", description = "Record type", required = true, in = ParameterIn.PATH) @PathVariable("recordType") String recordType,
        @Parameter(name = "recordId", description = "Record id", required = true, in = ParameterIn.PATH) @PathVariable("recordId") String recordId,
        @Parameter(name = "EvaluateExpressionsWithArrayRequestServerModel", description = "Expressions to evaluate", required = true) @Valid @RequestBody EvaluateExpressionsWithArrayRequestServerModel evaluateExpressionsWithArrayRequestServerModel
    ) {
        return new ResponseEntity<>(HttpStatus.NOT_IMPLEMENTED);

    }

}
