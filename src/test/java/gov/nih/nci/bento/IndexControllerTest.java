package gov.nih.nci.bento;

import gov.nih.nci.bento.controller.IndexController;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.result.MockMvcResultMatchers;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import static org.junit.Assert.assertNotNull;

@RunWith(SpringRunner.class)
public class IndexControllerTest {

    private MockMvc mockMvc;

    @Before
    public void setup() {
        this.mockMvc = MockMvcBuilders.standaloneSetup(new IndexController()).build();
    }

    /**
     * Confirm that the "/ping" endpoint accept GET requests and verify the following within the response:
     *     Http Status Code is 200 (OK)
     *     Content Type is "text/plain;charset=ISO-8859-1"
     *     Content matches the String "pong"
     *
     * @throws Exception
     */
    @Test
    public void pingEndpointTestGET() throws Exception {
        MvcResult result = this.mockMvc.perform(MockMvcRequestBuilders.get("/ping"))
                .andExpect(MockMvcResultMatchers.status().isOk())
                .andExpect(MockMvcResultMatchers.content().contentType("text/plain;charset=ISO-8859-1"))
                .andExpect(MockMvcResultMatchers.content().string("pong"))
                .andReturn();
        //assert method to satisfy codacy requirement, this statement will not be reached if the test fails
        assertNotNull(result);
    }

    /**
     * Confirm that the "/ping" endpoint does NOT accept POST requests and verify the following within the response:
     *     Http Status Code is 405 (METHOD NOT ALLOWED)
     *
     * @throws Exception
     */
    @Test
    public void pingEndpointTestPOST() throws Exception {
        MvcResult result = this.mockMvc.perform(MockMvcRequestBuilders.post("/ping"))
                .andExpect(MockMvcResultMatchers.status().isMethodNotAllowed())
                .andReturn();
        //assert method to satisfy codacy requirement, this statement will not be reached if the test fails
        assertNotNull(result);
    }

    /**
     * Confirm that the "/ping" endpoint does NOT accept PUT requests and verify the following within the response:
     *     Http Status Code is 405 (METHOD NOT ALLOWED)
     *
     * @throws Exception
     */
    @Test
    public void pingEndpointTestPUT() throws Exception {
        MvcResult result = this.mockMvc.perform(MockMvcRequestBuilders.put("/ping"))
                .andExpect(MockMvcResultMatchers.status().isMethodNotAllowed())
                .andReturn();
        //assert method to satisfy codacy requirement, this statement will not be reached if the test fails
        assertNotNull(result);
    }

    /**
     * Confirm that the "/ping" endpoint does NOT accept DELETE requests and verify the following within the response:
     *     Http Status Code is 405 (METHOD NOT ALLOWED)
     *
     * @throws Exception
     */
    @Test
    public void pingEndpointTestDELETE() throws Exception {
        MvcResult result = this.mockMvc.perform(MockMvcRequestBuilders.delete("/ping"))
                .andExpect(MockMvcResultMatchers.status().isMethodNotAllowed())
                .andReturn();
        //assert method to satisfy codacy requirement, this statement will not be reached if the test fails
        assertNotNull(result);
    }

    /**
     * Confirm that the "/" (root) endpoint accepts GET requests and verify the following within the response:
     *     Http Status Code is 200 (OK)
     *     View name is "/index"
     *
     * @throws Exception
     */
    @Test
    public void rootEndpointTestGET() throws Exception {
        MvcResult result = this.mockMvc.perform(MockMvcRequestBuilders.get("/"))
                .andExpect(MockMvcResultMatchers.status().isOk())
                .andExpect(MockMvcResultMatchers.view().name("/index"))
                .andReturn();
        //assert method to satisfy codacy requirement, this statement will not be reached if the test fails
        assertNotNull(result);
    }

    /**
     * Confirm that the "/" (root) endpoint accepts POST requests and returns a successful response
     *
     * @throws Exception
     */
    @Test
    public void rootEndpointTestPOST() throws Exception {
        MvcResult result = this.mockMvc.perform(MockMvcRequestBuilders.post("/"))
                .andExpect(MockMvcResultMatchers.status().isOk())
                .andReturn();
        //assert method to satisfy codacy requirement, this statement will not be reached if the test fails
        assertNotNull(result);
    }

    /**
     * Confirm that the "/" (root) endpoint accepts PUT requests and returns a successful response
     *
     * @throws Exception
     */
    @Test
    public void rootEndpointTestPUT() throws Exception {
        MvcResult result = this.mockMvc.perform(MockMvcRequestBuilders.put("/"))
                .andExpect(MockMvcResultMatchers.status().isOk())
                .andReturn();
        //assert method to satisfy codacy requirement, this statement will not be reached if the test fails
        assertNotNull(result);
    }

    /**
     * Confirm that the "/" (root) endpoint accepts DELETE requests and returns a successful response
     *
     * @throws Exception
     */
    @Test
    public void rootEndpointTestDELETE() throws Exception {
        MvcResult result = this.mockMvc.perform(MockMvcRequestBuilders.delete("/"))
                .andExpect(MockMvcResultMatchers.status().isOk())
                .andReturn();
        //assert method to satisfy codacy requirement, this statement will not be reached if the test fails
        assertNotNull(result);
    }

}
