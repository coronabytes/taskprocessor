<template>
  <v-container class="fill-height">
      <!--<v-data-table :items="items" density="compact"
                    :headers="headers"
                    :items-per-page="0">
        <template #bottom></template>
      </v-data-table>-->
    <v-row>
      <v-col v-for="item in queues" cols="12" md="4" >
        <QueueCard queue="item"/>
      </v-col>
    </v-row>



  </v-container>
</template>

<script setup lang="ts">
  import axios from 'axios';
  import { ref } from 'vue'
  import QueueCard from "@/components/QueueCard.vue";

  const headers = [
    { title: 'Name', align: 'start', sortable: false, key: 'name' },
  ];

  const items = [
    {
      name: 'African Elephant',
      species: 'Loxodonta africana',
      diet: 'Herbivore',
      habitat: 'Savanna, Forests',
    },
  ];

  const queues = ref([])

  axios.get('/taskprocessor/api/queues')
    .then(response => {
      queues.value = response.data;
    })
    .catch(error => console.log(error))
</script>
